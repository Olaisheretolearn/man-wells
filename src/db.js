import { MongoClient } from "mongodb";

let client;
let db;

export async function connectDB({ mongoUri, dbName }) {
  if (!mongoUri) throw new Error("MONGO_URI missing");
  if (!dbName) throw new Error("DB_NAME missing");

  client = new MongoClient(mongoUri);
  await client.connect();
  db = client.db(dbName);

  return db;
}

export function getDB() {
  if (!db) throw new Error("DB not initialized. Call connectDB() first.");
  return db;
}

export async function closeDB() {
  if (client) await client.close();
}
