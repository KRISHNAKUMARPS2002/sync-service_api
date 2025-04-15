const express = require("express");
const { Client } = require("pg");
const bodyParser = require("body-parser");
const dotenv = require("dotenv");
const app = express();

// Load environment variables
dotenv.config();

// Middleware
app.use(bodyParser.json());

// Database connection function
async function connectToDatabase() {
  const client = new Client({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    ssl: process.env.PG_SSL === "true",
  });

  await client.connect();
  return client;
}

// Authentication route - provide credentials
app.post("/auth/credentials", async (req, res) => {
  const { clientId, accessToken } = req.body;

  if (!clientId || !accessToken) {
    return res.status(400).json({ error: "Missing clientId or accessToken" });
  }

  try {
    const pgClient = await connectToDatabase();

    // Retrieve the credentials using the access token
    const credResult = await pgClient.query(
      "SELECT db_user, db_password FROM sync_users WHERE client_id = $1 AND access_token = $2",
      [clientId, accessToken]
    );

    await pgClient.end();

    if (credResult.rowCount === 0) {
      return res
        .status(401)
        .json({ error: "Invalid client ID or access token" });
    }

    // Return only database credentials, not server details
    res.json({
      dbUser: credResult.rows[0].db_user,
      dbPassword: credResult.rows[0].db_password,
    });
  } catch (error) {
    console.error(`Error retrieving credentials: ${error.message}`);
    res.status(500).json({ error: "Server error" });
  }
});

// Data sync route
app.post("/sync/data", async (req, res) => {
  const { clientId, accessToken, data } = req.body;

  if (!clientId || !accessToken || !data) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const pgClient = await connectToDatabase();

    // Verify client exists and token is valid
    const clientCheck = await pgClient.query(
      "SELECT client_id FROM sync_users WHERE client_id = $1 AND access_token = $2",
      [clientId, accessToken]
    );

    if (clientCheck.rowCount === 0) {
      await pgClient.end();
      return res
        .status(401)
        .json({ error: "Invalid client ID or access token" });
    }

    // Remove existing records for this client
    await pgClient.query("DELETE FROM rrc_clients WHERE client_id = $1", [
      clientId,
    ]);

    // Insert new data
    let recordCount = 0;
    for (const row of data) {
      await pgClient.query(
        "INSERT INTO rrc_clients (code, name, address, branch, client_id) VALUES ($1, $2, $3, $4, $5)",
        [
          row.CODE || row.code,
          row.NAME || row.name,
          row.ADDRESS || row.address,
          row.BRANCH || row.branch,
          clientId,
        ]
      );
      recordCount++;
    }

    // Log sync operation
    await pgClient.query(
      "INSERT INTO sync_logs (client_id, records_synced, status, message) VALUES ($1, $2, $3, $4)",
      [clientId, recordCount, "SUCCESS", "Sync completed successfully"]
    );

    await pgClient.end();

    res.json({
      success: true,
      message: `Successfully synced ${recordCount} records`,
      recordCount,
    });
  } catch (error) {
    console.error(`Error syncing data: ${error.message}`);

    // Try to log the error
    try {
      const pgClient = await connectToDatabase();
      await pgClient.query(
        "INSERT INTO sync_logs (client_id, records_synced, status, message) VALUES ($1, $2, $3, $4)",
        [clientId, 0, "FAILED", error.message]
      );
      await pgClient.end();
    } catch (logError) {
      console.error(`Failed to log error: ${logError.message}`);
    }

    res.status(500).json({ error: "Server error" });
  }
});

// Log sync operation
app.post("/sync/log", async (req, res) => {
  const { clientId, accessToken, status, recordCount, message } = req.body;

  if (!clientId || !accessToken || !status) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const pgClient = await connectToDatabase();

    // Verify client exists and token is valid
    const clientCheck = await pgClient.query(
      "SELECT client_id FROM sync_users WHERE client_id = $1 AND access_token = $2",
      [clientId, accessToken]
    );

    if (clientCheck.rowCount === 0) {
      await pgClient.end();
      return res
        .status(401)
        .json({ error: "Invalid client ID or access token" });
    }

    // Log sync operation
    await pgClient.query(
      "INSERT INTO sync_logs (client_id, records_synced, status, message) VALUES ($1, $2, $3, $4)",
      [clientId, recordCount || 0, status, message || ""]
    );

    await pgClient.end();

    res.json({ success: true });
  } catch (error) {
    console.error(`Error logging sync: ${error.message}`);
    res.status(500).json({ error: "Server error" });
  }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
