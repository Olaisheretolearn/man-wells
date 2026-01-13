import { MongoClient } from "mongodb";
import "dotenv/config";

const client = new MongoClient(process.env.MONGO_URI);

await client.connect();
console.log("Connected OK");
await client.close();
