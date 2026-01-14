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

app.set("trust proxy", 1);


app.use((req, res, next) => {
  if (req.method === "GET") res.setHeader("Cache-Control", "public, max-age=60");
  next();
});

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




app.set("trust proxy", 1); 




  app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});


  app.listen(PORT, () => {
    console.log(`API running on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error("Startup error:", err);
  process.exit(1);
});
