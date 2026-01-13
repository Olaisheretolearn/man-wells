import "dotenv/config";
import express from "express";
import cors from "cors";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import { connectDB } from "./db.js";
import { wellsRouter } from "./routes.wells.js";

const app = express();

app.use(helmet());
app.use(cors({ origin: true }));
app.use(express.json({ limit: "1mb" }));

app.use(rateLimit({
  windowMs: 60 * 1000,
  limit: 120,
  standardHeaders: "draft-7",
  legacyHeaders: false
}));

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.use("/wells", wellsRouter);

const PORT = process.env.PORT || 3000;

async function start() {
  await connectDB({
    mongoUri: process.env.MONGO_URI,
    dbName: process.env.DB_NAME || "gis"
  });

  app.listen(PORT, () => {
    console.log(`API running on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
