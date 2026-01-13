import express from "express";
import { z } from "zod";
import { ObjectId } from "mongodb";
import { getDB } from "./db.js";

export const wellsRouter = express.Router();

function wellsCol() {
  const db = getDB();
  const name = process.env.WELLS_COLLECTION || "wells";
  return db.collection(name);
}




wellsRouter.get("/near/search", async (req, res) => {
  const schema = z.object({
    lon: z.coerce.number().min(-180).max(180),
    lat: z.coerce.number().min(-90).max(90),
    maxDistance: z.coerce.number().min(1).max(200000).default(10000),
    limit: z.coerce.number().min(1).max(500).default(50)
  });

  const parsed = schema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid query", details: parsed.error.flatten() });
  }

  const { lon, lat, maxDistance, limit } = parsed.data;

  const cursor = wellsCol().find(
    {
      location: {
        $near: {
          $geometry: { type: "Point", coordinates: [lon, lat] },
          $maxDistance: maxDistance
        }
      }
    },
    {
      projection: {
        licence: 1,
        name: 1,
        company: 1,
        map_status: 1,
        status: 1,
        status_date: 1,
        mineral_ri: 1,
        techdoc_url: 1,
        location: 1
      }
    }
  ).limit(limit);

  const results = await cursor.toArray();
  res.json({ count: results.length, results });
});


wellsRouter.get("/", async (req, res) => {
  const schema = z.object({
    bbox: z.string().optional(), 
    limit: z.coerce.number().min(1).max(5000).default(500)
  });

  const parsed = schema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid query", details: parsed.error.flatten() });
  }

  const { bbox, limit } = parsed.data;

  const query = {};
  if (bbox) {
    const parts = bbox.split(",").map(Number);
    if (parts.length !== 4 || parts.some(Number.isNaN)) {
      return res.status(400).json({ error: "bbox must be minLon,minLat,maxLon,maxLat" });
    }
    const [minLon, minLat, maxLon, maxLat] = parts;

    query.location = {
      $geoWithin: {
        $box: [
          [minLon, minLat],
          [maxLon, maxLat]
        ]
      }
    };
  }

  const cursor = wellsCol().find(query, {
    projection: {
      licence: 1,
      name: 1,
      company: 1,
      map_status: 1,
      location: 1
    }
  }).limit(limit);

  const results = await cursor.toArray();
  res.json({ count: results.length, results });
});


wellsRouter.get("/search", async (req, res) => {
  const schema = z.object({
    q: z.string().min(1),
    limit: z.coerce.number().min(1).max(50).default(20)
  });

  const parsed = schema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid query", details: parsed.error.flatten() });
  }

  const { q, limit } = parsed.data;
  const qTrim = q.trim();

  const col = wellsCol();


  const asNumber = /^[0-9]+$/.test(qTrim) ? Number(qTrim) : null;

  const filter = asNumber !== null
    ? { $or: [{ licence: asNumber }, { name: { $regex: qTrim, $options: "i" } }, { company: { $regex: qTrim, $options: "i" } }] }
    : { $or: [{ name: { $regex: qTrim, $options: "i" } }, { company: { $regex: qTrim, $options: "i" } }] };

  const results = await col.find(filter, {
    projection: {
      licence: 1,
      name: 1,
      company: 1,
      map_status: 1,
      easting: 1,
      northing: 1,
      location: 1
    }
  }).limit(limit).toArray();

  res.json({ q: qTrim, count: results.length, results });
});




wellsRouter.get("/licence/:licence", async (req, res) => {
  const licence = Number(req.params.licence);
  if (!Number.isFinite(licence)) {
    return res.status(400).json({ error: "Invalid licence" });
  }

  const doc = await wellsCol().findOne({ licence });
  if (!doc) return res.status(404).json({ error: "Not found" });

  res.json(doc);
});



wellsRouter.get("/geojson", async (req, res) => {
  const schema = z.object({
    bbox: z.string().optional(),
    limit: z.coerce.number().min(1).max(5000).default(2000)
  });

  const parsed = schema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid query", details: parsed.error.flatten() });
  }

  const { bbox, limit } = parsed.data;

  const query = {};
  if (bbox) {
    const parts = bbox.split(",").map(Number);
    if (parts.length !== 4 || parts.some(Number.isNaN)) {
      return res.status(400).json({ error: "bbox must be minLon,minLat,maxLon,maxLat" });
    }
    const [minLon, minLat, maxLon, maxLat] = parts;

    query.location = {
      $geoWithin: {
        $box: [
          [minLon, minLat],
          [maxLon, maxLat]
        ]
      }
    };
  }

  const docs = await wellsCol().find(query, {
    projection: {
      licence: 1,
      name: 1,
      company: 1,
      map_status: 1,
      mineral_ri: 1,
      status: 1,
      status_date: 1,
      techdoc_url: 1,
      location: 1
    }
  }).limit(limit).toArray();

  const features = docs
    .filter(d => d.location && d.location.type === "Point" && Array.isArray(d.location.coordinates))
    .map(d => ({
      type: "Feature",
      id: String(d._id),
      geometry: d.location,
      properties: {
        _id: String(d._id),
        licence: d.licence,
        name: d.name,
        company: d.company,
        map_status: d.map_status,
        mineral_ri: d.mineral_ri,
        status: d.status,
        status_date: d.status_date,
        techdoc_url: d.techdoc_url
      }
    }));

  res.json({
    type: "FeatureCollection",
    count: features.length,
    features
  });
});


wellsRouter.get("/random", async (req, res) => {
  const schema = z.object({
    map_status: z.string().optional(),
    status: z.string().optional()
  });

  const parsed = schema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: "Invalid query" });

  const { map_status, status } = parsed.data;

  const match = {};
  if (map_status) match.map_status = map_status;
  if (status) match.status = status;

  const [doc] = await wellsCol().aggregate([
    { $match: match },
    { $sample: { size: 1 } }
  ]).toArray();

  if (!doc) return res.status(404).json({ error: "No matching well found" });
  res.json(doc);
});


wellsRouter.get("/licence/:licence/similar/company", async (req, res) => {
  const licence = Number(req.params.licence);
  const limit = Math.min(Number(req.query.limit ?? 20), 200);

  if (!Number.isFinite(licence)) return res.status(400).json({ error: "Invalid licence" });

  const base = await wellsCol().findOne({ licence }, { projection: { company: 1 } });
  if (!base) return res.status(404).json({ error: "Base well not found" });

  const results = await wellsCol().find(
    { company: base.company, licence: { $ne: licence } },
    { projection: { licence: 1, name: 1, company: 1, map_status: 1, location: 1 } }
  ).limit(limit).toArray();

  res.json({ licence, company: base.company, count: results.length, results });
});

wellsRouter.get("/licence/:licence/similar/radius", async (req, res) => {
  const licence = Number(req.params.licence);
  const maxDistance = Math.min(Number(req.query.maxDistance ?? 10000), 200000);
  const limit = Math.min(Number(req.query.limit ?? 50), 500);

  if (!Number.isFinite(licence)) return res.status(400).json({ error: "Invalid licence" });

  const base = await wellsCol().findOne(
    { licence },
    { projection: { licence: 1, location: 1, name: 1 } }
  );
  if (!base) return res.status(404).json({ error: "Base well not found" });
  if (!base.location?.coordinates) return res.status(400).json({ error: "Base well missing location" });

  const [lon, lat] = base.location.coordinates;

  const results = await wellsCol().find(
    {
      licence: { $ne: licence },
      location: {
        $near: {
          $geometry: { type: "Point", coordinates: [lon, lat] },
          $maxDistance: maxDistance
        }
      }
    },
    { projection: { licence: 1, name: 1, company: 1, map_status: 1, location: 1 } }
  ).limit(limit).toArray();

  res.json({
    base: { licence: base.licence, name: base.name, lon, lat },
    maxDistance,
    count: results.length,
    results
  });
});








wellsRouter.get("/:id", async (req, res) => {
  const { id } = req.params;

  let _id;
  try {
    _id = new ObjectId(id);
  } catch {
    return res.status(400).json({ error: "Invalid id" });
  }

  const doc = await wellsCol().findOne({ _id });
  if (!doc) return res.status(404).json({ error: "Not found" });

  res.json(doc);
});


