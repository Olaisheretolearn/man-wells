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


//poly helpers
function closeRingIfNeeded(ring) {
  if (ring.length < 4) return ring;
  const first = ring[0];
  const last = ring[ring.length - 1];
  const same = first[0] === last[0] && first[1] === last[1];
  return same ? ring : [...ring, first];
}

function clampLonLat([lon, lat]) {
  return [
    Math.max(-180, Math.min(180, lon)),
    Math.max(-90, Math.min(90, lat))
  ];
}

// Expecting: { coordinates: [[ [lon,lat], [lon,lat], ... ]] }
const polygonBodySchema = z.object({
  coordinates: z.array(
    z.array(
      z.tuple([z.number(), z.number()]) 
    ).min(3) // at least triangle before closing
  ).min(1),
  limit: z.coerce.number().min(1).max(5000).default(2000)
});

// A) Query wells inside polygon
wellsRouter.post("/polygon/query", async (req, res) => {
  const parsed = polygonBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid body", details: parsed.error.flatten() });
  }

  const { coordinates, limit } = parsed.data;

  // Use first ring only (outer boundary)
  let ring = coordinates[0].map(clampLonLat);
  ring = closeRingIfNeeded(ring);

  if (ring.length < 4) {
    return res.status(400).json({ error: "Polygon ring must have at least 3 points (and will be closed)." });
  }

  const query = {
    location: {
      $geoWithin: {
        $geometry: {
          type: "Polygon",
          coordinates: [ring]
        }
      }
    }
  };

  const results = await wellsCol().find(query, {
    projection: {
      licence: 1,
      name: 1,
      company: 1,
      map_status: 1,
      status: 1,
      status_date: 1,
      mineral_ri: 1,
      deviation: 1,
      techdoc_url: 1,
      location: 1
    }
  }).limit(limit).toArray();

  res.json({ count: results.length, results });
});



wellsRouter.post("/polygon/stats", async (req, res) => {
  const parsed = polygonBodySchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid body", details: parsed.error.flatten() });
  }

  const { coordinates } = parsed.data;

  let ring = coordinates[0].map(clampLonLat);
  ring = closeRingIfNeeded(ring);

  if (ring.length < 4) {
    return res.status(400).json({ error: "Polygon ring must have at least 3 points (and will be closed)." });
  }

  const within = {
    $geoWithin: {
      $geometry: { type: "Polygon", coordinates: [ring] }
    }
  };

  const pipeline = [
    { $match: { location: within } },

   
    {
      $addFields: {
        company_norm: { $ifNull: ["$company", "Unknown"] },
        status_norm: { $ifNull: ["$status", "Unknown"] },
        deviation_norm: { $ifNull: ["$deviation", "Unknown"] },
        mineral_ri_norm: { $ifNull: ["$mineral_ri", "Unknown"] }
      }
    },

    {
      $facet: {
        top_companies: [
          { $group: { _id: "$company_norm", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $limit: 15 },
          { $project: { _id: 0, company: "$_id", count: 1 } }
        ],

        deviation_vs_outcome: [
          { $group: { _id: { deviation: "$deviation_norm", status: "$status_norm" }, count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $project: { _id: 0, deviation: "$_id.deviation", status: "$_id.status", count: 1 } }
        ],

        mineral_rights_split: [
          { $group: { _id: "$mineral_ri_norm", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $project: { _id: 0, mineral_ri: "$_id", count: 1 } }
        ],

        map_status_split: [
          { $group: { _id: { $ifNull: ["$map_status", "Unknown"] }, count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $project: { _id: 0, map_status: "$_id", count: 1 } }
        ],

        raw_for_metrics: [
  {
    $project: {
      _id: 0,
      company: "$company_norm",
      mineral_ri: "$mineral_ri_norm",
      status_date: "$status_date",
      coords: "$location.coordinates"
    }
  },
  { $limit: 10000 }
],





     
        status_date_summary: [
          {
            $group: {
              _id: null,
              min_status_date: { $min: "$status_date" },
              max_status_date: { $max: "$status_date" }
            }
          },
          { $project: { _id: 0, min_status_date: 1, max_status_date: 1 } }
        ],

        total: [{ $count: "count" }]
      }
    }
  ];

  const [out] = await wellsCol().aggregate(pipeline, { allowDiskUse: true }).toArray();

  const rows = out?.raw_for_metrics ?? [];
const count = out?.total?.[0]?.count ?? 0;

// ---- FREEHOLD vs Crown ratio (use mineral_rights_split you already return) ----
// We'll also compute percent in response for convenience
const mineral_rights_split = (out.mineral_rights_split ?? []).map(r => ({
  ...r,
  pct: count ? r.count / count : 0
}));

// ---- Median status_date ----
const dates = rows.map(r => r.status_date).filter(Boolean);
const median_status_date = medianDate(dates);
const median_status_year = median_status_date ? median_status_date.getUTCFullYear() : null;

// ---- Points ----
const points = rows
  .map(r => r.coords)
  .filter(c => Array.isArray(c) && c.length === 2 && Number.isFinite(c[0]) && Number.isFinite(c[1]));

// ---- Area, density ----
const area_m2 = polygonAreaMeters2(ring);
const lambda = area_m2 > 0 ? points.length / area_m2 : null; // per m²

// ---- NND + NNI (Clark–Evans) ----
const cap = points.length > 4000 ? 1200 : 1500;
const nnd = meanNearestNeighborDistance(points, cap);
const mean_nnd_m = nnd.mean_m;

let expected_mean_nnd_m = null;
let nni = null;

if (mean_nnd_m != null && lambda != null && lambda > 0) {
  expected_mean_nnd_m = 1 / (2 * Math.sqrt(lambda));
  nni = mean_nnd_m / expected_mean_nnd_m;
}

// ---- HHI ----
const companyCounts = new Map();
for (const r of rows) {
  const key = r.company ?? "Unknown";
  companyCounts.set(key, (companyCounts.get(key) ?? 0) + 1);
}

let hhi = null;
if (count > 0) {
  let sum = 0;
  for (const c of companyCounts.values()) {
    const share = c / count;
    sum += share * share;
  }
  hhi = sum;
}


res.json({
  polygon: { type: "Polygon", coordinates: [ring] },

  // existing
  count,
  top_companies: out.top_companies ?? [],
  deviation_vs_outcome: out.deviation_vs_outcome ?? [],
  mineral_rights_split,
  map_status_split: out.map_status_split ?? [],
  status_date_summary: out.status_date_summary?.[0] ?? null,

  // NEW metrics
  median_status_date,
  median_status_year,

  area_m2,
  wells_with_coords: points.length,

  mean_nnd_m,
  expected_mean_nnd_m,
  nni,
  nnd_used_n: nnd.used_n,
  nnd_capped: nnd.capped,

  hhi
});

});


function haversineMeters(aLonLat, bLonLat) {
  const R = 6371000; // meters
  const toRad = (d) => (d * Math.PI) / 180;

  const [lon1, lat1] = aLonLat;
  const [lon2, lat2] = bLonLat;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const lat1r = toRad(lat1);
  const lat2r = toRad(lat2);

  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1r) * Math.cos(lat2r) * Math.sin(dLon / 2) ** 2;

  return 2 * R * Math.asin(Math.sqrt(h));
}

// Geodesic polygon area (m²) from lon/lat ring (closed)
function polygonAreaMeters2(ringLonLat) {
  const R = 6378137; // meters
  const toRad = (d) => (d * Math.PI) / 180;

  let sum = 0;
  for (let i = 0; i < ringLonLat.length - 1; i++) {
    const [lon1, lat1] = ringLonLat[i];
    const [lon2, lat2] = ringLonLat[i + 1];

    const lon1r = toRad(lon1);
    const lon2r = toRad(lon2);
    const lat1r = toRad(lat1);
    const lat2r = toRad(lat2);

    sum += (lon2r - lon1r) * (2 + Math.sin(lat1r) + Math.sin(lat2r));
  }
  return Math.abs(sum) * (R * R) / 2;
}

function medianDate(dates) {
  const times = dates
    .map((d) => new Date(d).getTime())
    .filter((t) => Number.isFinite(t))
    .sort((a, b) => a - b);

  if (!times.length) return null;

  const mid = Math.floor(times.length / 2);
  const med =
    times.length % 2 === 1
      ? times[mid]
      : Math.round((times[mid - 1] + times[mid]) / 2);

  return new Date(med);
}

// O(n^2), so cap to avoid melting
function meanNearestNeighborDistance(pointsLonLat, cap = 1500) {
  const n0 = pointsLonLat.length;
  if (n0 < 2) return { mean_m: null, used_n: n0, capped: false };

  let pts = pointsLonLat;
  let capped = false;

  if (n0 > cap) {
    capped = true;
    // random sample
    const shuffled = [...pointsLonLat];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    pts = shuffled.slice(0, cap);
  }

  const n = pts.length;
  let total = 0;

  for (let i = 0; i < n; i++) {
    let best = Infinity;
    for (let j = 0; j < n; j++) {
      if (i === j) continue;
      const d = haversineMeters(pts[i], pts[j]);
      if (d < best) best = d;
    }
    total += best;
  }

  return { mean_m: total / n, used_n: n, capped };
}









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


