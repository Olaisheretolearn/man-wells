import express from "express";
import { z } from "zod";
import { getDB } from "./db.js";

export const statsRouter = express.Router();

function wellsCol() {
  const db = getDB();
  return db.collection(process.env.WELLS_COLLECTION || "wells");
}



statsRouter.get("/status", async (req, res) => {
  const results = await wellsCol().aggregate([
    { $group: { _id: "$map_status", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
  ]).toArray();

  res.json({
    metric: "count_by_map_status",
    results: results.map(r => ({ map_status: r._id ?? "Unknown", count: r.count }))
  });
});

statsRouter.get("/top-companies", async (req, res) => {
  const metric = String(req.query.metric ?? "wells");
  const limit = Math.min(Number(req.query.limit ?? 20), 100);

  let match = {};
  if (metric === "abandoned") {
    // If you want "abandoned" as: map_status contains 'abandon'
    match = { map_status: { $regex: "abandon", $options: "i" } };
  }

  const results = await wellsCol().aggregate([
    { $match: match },
    { $group: { _id: "$company", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: limit }
  ]).toArray();

  res.json({
    metric,
    results: results.map(r => ({ company: r._id ?? "Unknown", count: r.count }))
  });
});


statsRouter.get("/closest-birthday", async (req, res) => {
  const month = Number(req.query.month);
  const day = Number(req.query.day);
  const limit = Math.min(Number(req.query.limit ?? 10), 50);

  if (!Number.isFinite(month) || !Number.isFinite(day)) {
    return res.status(400).json({ error: "month and day are required numbers" });
  }

  // Get docs that have status_date
  const docs = await wellsCol().find(
    { status_date: { $type: "string" } },
    { projection: { licence: 1, name: 1, company: 1, map_status: 1, status_date: 1, location: 1 } }
  ).limit(5000).toArray(); 

  function dayOfYear(m, d) {
    const date = new Date(Date.UTC(2001, m - 1, d)); 
    const start = new Date(Date.UTC(2001, 0, 1));
    return Math.floor((date - start) / 86400000) + 1;
  }

  const target = dayOfYear(month, day);

  function parseMonthDay(s) {
    // "1984-12-14 00:00:00"
    const m = Number(s.slice(5, 7));
    const d = Number(s.slice(8, 10));
    if (!Number.isFinite(m) || !Number.isFinite(d)) return null;
    return { m, d };
  }

  function circularDiff(a, b) {
    const diff = Math.abs(a - b);
    return Math.min(diff, 365 - diff);
  }

  const ranked = docs
    .map(doc => {
      const md = parseMonthDay(doc.status_date);
      if (!md) return null;
      const doy = dayOfYear(md.m, md.d);
      return { doc, distance: circularDiff(doy, target), month: md.m, day: md.d };
    })
    .filter(Boolean)
    .sort((x, y) => x.distance - y.distance)
    .slice(0, limit)
    .map(x => ({
      distance_days: x.distance,
      status_month: x.month,
      status_day: x.day,
      ...x.doc,
      _id: String(x.doc._id)
    }));

  res.json({ month, day, count: ranked.length, results: ranked });
});

