import { useState, useEffect, useRef } from "react";
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";

// ─── Design System ─────────────────────────────────────────────────────────────
const C = {
  bg: "#07070f",
  surface: "#0d0d1c",
  surfaceHigh: "#121225",
  border: "#1a1a30",
  borderHigh: "#282845",
  accent: "#00e887",
  accentDim: "#00e88712",
  orange: "#ff6b2b",
  orangeDim: "#ff6b2b12",
  blue: "#4d9fff",
  blueDim: "#4d9fff12",
  purple: "#a855f7",
  purpleDim: "#a855f712",
  yellow: "#fbbf24",
  yellowDim: "#fbbf2412",
  red: "#f43f5e",
  redDim: "#f43f5e12",
  teal: "#22d3ee",
  text: "#ddddf0",
  textMid: "#8888aa",
  textDim: "#484868",
};

const fmt = {
  currency: (v) =>
    v >= 1_000_000 ? `$${(v / 1_000_000).toFixed(2)}M`
    : v >= 1_000   ? `$${(v / 1_000).toFixed(1)}k`
    :                `$${(v || 0).toFixed(2)}`,
  num: (v) =>
    v >= 1_000_000 ? `${(v / 1_000_000).toFixed(1)}M`
    : v >= 1_000   ? `${(v / 1_000).toFixed(1)}k`
    :                (v || 0).toLocaleString(),
  ts: (v) => new Date(v * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
  uptime: (s) => {
    s = Math.round(s || 0);
    if (s < 60)   return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  },
};

// ─── WebSocket ─────────────────────────────────────────────────────────────────
function useWebSocket(url) {
  const [data, setData]           = useState(null);
  const [connected, setConnected] = useState(false);
  const [flashSale, setFlashSale] = useState(false);
  const ws = useRef(null);

  useEffect(() => {
    const connect = () => {
      ws.current = new WebSocket(url);
      ws.current.onopen  = () => setConnected(true);
      ws.current.onclose = () => { setConnected(false); setTimeout(connect, 2000); };
      ws.current.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        setData(msg);
        if (msg.simulator?.flash_sale_active !== undefined)
          setFlashSale(msg.simulator.flash_sale_active);
      };
    };
    connect();
    return () => ws.current?.close();
  }, [url]);

  return { data, connected, flashSale };
}

// ─── Animated Value ────────────────────────────────────────────────────────────
function AnimatedValue({ value, format, color = C.text }) {
  const [bump, setBump] = useState(false);
  const prev = useRef(value);

  useEffect(() => {
    if (value !== prev.current) {
      setBump(true);
      setTimeout(() => setBump(false), 300);
      prev.current = value;
    }
  }, [value]);

  return (
    <span style={{
      color,
      display: "inline-block",
      transition: "transform 0.3s ease",
      transform: bump ? "scale(1.06)" : "scale(1)",
      textShadow: bump ? `0 0 24px ${color}88` : "none",
    }}>
      {format(value)}
    </span>
  );
}

// ─── KPI Card ─────────────────────────────────────────────────────────────────
function KPICard({ label, value, format, color, sub, icon }) {
  return (
    <div className="card" style={{
      background: C.surface,
      border: `1px solid ${C.border}`,
      borderRadius: 12,
      padding: "18px 22px",
      position: "relative",
      overflow: "hidden",
      transition: "border-color 0.2s, box-shadow 0.2s",
    }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 2,
        background: `linear-gradient(90deg, ${color}, ${color}44)`,
      }} />
      <div style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 8 }}>
        {icon && <span style={{ marginRight: 5 }}>{icon}</span>}{label}
      </div>
      <div style={{ fontSize: 26, fontWeight: 800, fontFamily: "'DM Mono', monospace", letterSpacing: "-0.02em" }}>
        <AnimatedValue value={value} format={format} color={color} />
      </div>
      {sub && <div style={{ color: C.textDim, fontSize: 11, marginTop: 6, fontFamily: "'Space Mono', monospace" }}>{sub}</div>}
    </div>
  );
}

// ─── Section Header ────────────────────────────────────────────────────────────
function SectionHeader({ title, sub, color = C.accent }) {
  return (
    <div style={{ marginBottom: 14, display: "flex", alignItems: "baseline", gap: 12 }}>
      <div style={{
        width: 3, height: 14, background: color, borderRadius: 2,
        flexShrink: 0, alignSelf: "center",
      }} />
      <div>
        <div style={{ color: C.text, fontSize: 12, fontWeight: 700, fontFamily: "'Space Mono', monospace", letterSpacing: "0.1em" }}>
          {title}
        </div>
        {sub && <div style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", marginTop: 2, lineHeight: 1.5 }}>{sub}</div>}
      </div>
    </div>
  );
}

// ─── Custom Chart Tooltip ──────────────────────────────────────────────────────
function ChartTooltip({ active, payload, label, rows }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: C.surfaceHigh, border: `1px solid ${C.borderHigh}`,
      borderRadius: 8, padding: "8px 12px", fontFamily: "'Space Mono', monospace", fontSize: 11,
    }}>
      <div style={{ color: C.textDim, marginBottom: 6 }}>{fmt.ts(label)}</div>
      {rows ? rows(payload) : payload.map((p, i) => (
        <div key={i} style={{ color: p.color || C.accent }}>{p.value}</div>
      ))}
    </div>
  );
}

// ─── Conversion Funnel ─────────────────────────────────────────────────────────
function FunnelChart({ funnel }) {
  if (!funnel) return null;
  const stages = [
    { key: "page_view",      label: "Page View",   color: C.blue },
    { key: "product_click",  label: "Click",       color: C.purple },
    { key: "add_to_cart",    label: "Add to Cart", color: C.yellow },
    { key: "checkout_start", label: "Checkout",    color: C.orange },
    { key: "purchase",       label: "Purchase",    color: C.accent },
  ];
  const max = Math.max(...stages.map((s) => funnel[s.key] || 0), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {stages.map((stage, i) => {
        const val = funnel[stage.key] || 0;
        const pct = (val / max) * 100;
        const conv = i > 0 ? ((val / (funnel[stages[i - 1].key] || 1)) * 100).toFixed(1) : null;
        return (
          <div key={stage.key}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ color: C.textMid, fontSize: 11, fontFamily: "'Space Mono', monospace" }}>{stage.label}</span>
              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                {conv && <span style={{ color: C.textDim, fontSize: 10 }}>↓{conv}%</span>}
                <span style={{ color: stage.color, fontSize: 11, fontWeight: 700, fontFamily: "'DM Mono', monospace" }}>{fmt.num(val)}</span>
              </div>
            </div>
            <div style={{ height: 18, background: C.border, borderRadius: 4, overflow: "hidden" }}>
              <div style={{
                height: "100%", width: `${pct}%`,
                background: stage.color, borderRadius: 4, opacity: 0.8,
                transition: "width 0.6s cubic-bezier(0.4,0,0.2,1)",
                boxShadow: `0 0 10px ${stage.color}44`,
              }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Top Products ──────────────────────────────────────────────────────────────
function TopProducts({ products }) {
  if (!products?.length) return null;
  const max = products[0]?.[1] || 1;
  const COLORS = [C.accent, C.blue, C.purple, C.orange, C.yellow, C.teal, C.red, C.accent];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {products.slice(0, 8).map(([name, count], i) => (
        <div key={name}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
            <span style={{
              color: C.textMid, fontSize: 11, fontFamily: "'Space Mono', monospace",
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "68%",
            }}>{name}</span>
            <span style={{ color: COLORS[i], fontSize: 11, fontWeight: 700, fontFamily: "'DM Mono', monospace" }}>{fmt.num(count)}</span>
          </div>
          <div style={{ height: 5, background: C.border, borderRadius: 3, overflow: "hidden" }}>
            <div style={{
              height: "100%", width: `${(count / max) * 100}%`,
              background: COLORS[i], borderRadius: 3,
              transition: "width 0.5s ease",
              boxShadow: `0 0 6px ${COLORS[i]}44`,
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Geo + Device ─────────────────────────────────────────────────────────────
function GeoDevice({ geo, deviceDist }) {
  const geoTotal = (geo || []).reduce((s, [, v]) => s + v, 0) || 1;
  const GEO_COLORS = [C.accent, C.blue, C.purple, C.orange, C.yellow, C.teal, C.red];

  const deviceData = Object.entries(deviceDist || {}).map(([name, value]) => ({ name, value }));
  const deviceTotal = deviceData.reduce((s, d) => s + d.value, 0) || 1;
  const DEV_COLORS = [C.blue, C.accent, C.purple];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <div>
        <div style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.12em", marginBottom: 10 }}>
          TRAFFIC BY COUNTRY
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
          {(geo || []).slice(0, 6).map(([country, count], i) => {
            const pct = ((count / geoTotal) * 100).toFixed(1);
            return (
              <div key={country} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ color: C.textMid, fontSize: 11, fontFamily: "'Space Mono', monospace", width: 26 }}>{country}</span>
                <div style={{ flex: 1, height: 7, background: C.border, borderRadius: 4, overflow: "hidden" }}>
                  <div style={{
                    height: "100%", width: `${pct}%`,
                    background: GEO_COLORS[i], borderRadius: 4,
                    transition: "width 0.6s ease",
                    boxShadow: `0 0 6px ${GEO_COLORS[i]}44`,
                  }} />
                </div>
                <span style={{ color: GEO_COLORS[i], fontSize: 10, fontFamily: "'DM Mono', monospace", width: 40, textAlign: "right" }}>{pct}%</span>
              </div>
            );
          })}
        </div>
      </div>

      <div>
        <div style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.12em", marginBottom: 10 }}>
          DEVICE BREAKDOWN
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
          {deviceData.map((d, i) => {
            const pct = ((d.value / deviceTotal) * 100).toFixed(1);
            return (
              <div key={d.name} style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ color: C.textMid, fontSize: 11, fontFamily: "'Space Mono', monospace", width: 52 }}>{d.name}</span>
                <div style={{ flex: 1, height: 7, background: C.border, borderRadius: 4, overflow: "hidden" }}>
                  <div style={{
                    height: "100%", width: `${pct}%`,
                    background: DEV_COLORS[i], borderRadius: 4,
                    transition: "width 0.5s ease",
                    boxShadow: `0 0 6px ${DEV_COLORS[i]}44`,
                  }} />
                </div>
                <span style={{ color: DEV_COLORS[i], fontSize: 10, fontFamily: "'DM Mono', monospace", width: 40, textAlign: "right" }}>{pct}%</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ─── Partition Health ──────────────────────────────────────────────────────────
function PartitionHealth({ partitions }) {
  if (!partitions?.length) return null;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 8 }}>
      {partitions.map((p) => {
        const lagPct = Math.min(100, (p.lag / 10_000) * 100);
        const color = lagPct > 70 ? C.red : lagPct > 30 ? C.yellow : C.accent;
        return (
          <div key={p.partition_id} style={{
            background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 12px",
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 7 }}>
              <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace", letterSpacing: "0.1em" }}>
                P{p.partition_id}
              </span>
              <span style={{ color, fontSize: 9, fontFamily: "'DM Mono', monospace", fontWeight: 700 }}>
                {fmt.num(p.lag)} lag
              </span>
            </div>
            <div style={{ height: 5, background: C.border, borderRadius: 3, overflow: "hidden", marginBottom: 7 }}>
              <div style={{
                height: "100%", width: `${lagPct}%`,
                background: color, borderRadius: 3,
                transition: "width 0.5s ease, background 0.3s",
                boxShadow: `0 0 8px ${color}66`,
              }} />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>
                ↑{fmt.num(p.total_produced)}
              </span>
              <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>
                ↓{fmt.num(p.total_consumed)}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Shard Workers ─────────────────────────────────────────────────────────────
function ShardWorkers({ workers }) {
  if (!workers?.length) return null;
  const totalProcessed = workers.reduce((s, w) => s + w.processed, 0);

  return (
    <div>
      <SectionHeader
        title="SHARD WORKERS"
        sub={`${workers.length} workers — each owns a partition subset and drains in batches`}
        color={C.teal}
      />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(110px, 1fr))", gap: 8 }}>
        {workers.map((w) => {
          const lag = w.queue_lag || 0;
          const lagColor = lag > 5000 ? C.red : lag > 1000 ? C.yellow : C.accent;
          return (
            <div key={w.shard_id} style={{
              background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 12px",
            }}>
              <div style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace", letterSpacing: "0.08em", marginBottom: 6 }}>
                {w.shard_id.toUpperCase()}
              </div>
              <div style={{ color: C.text, fontSize: 14, fontWeight: 800, fontFamily: "'DM Mono', monospace" }}>
                {fmt.num(w.processed)}
              </div>
              <div style={{ color: C.textDim, fontSize: 9, marginTop: 2 }}>processed</div>
              <div style={{ height: 3, background: C.border, borderRadius: 2, margin: "8px 0 4px" }}>
                <div style={{
                  height: "100%", width: `${Math.min(100, (lag / 10_000) * 100)}%`,
                  background: lagColor, borderRadius: 2, transition: "width 0.5s ease",
                  boxShadow: `0 0 6px ${lagColor}66`,
                }} />
              </div>
              <div style={{ color: lagColor, fontSize: 9, fontFamily: "'DM Mono', monospace" }}>
                lag: {fmt.num(lag)}
              </div>
              <div style={{ color: C.textDim, fontSize: 9, marginTop: 2 }}>
                P[{w.partition_ids.join(",")}]
              </div>
            </div>
          );
        })}
      </div>
      <div style={{ color: C.textDim, fontSize: 10, marginTop: 10, fontFamily: "'Space Mono', monospace" }}>
        {fmt.num(totalProcessed)} total events processed across all shards
      </div>
    </div>
  );
}

// ─── Rate Control ──────────────────────────────────────────────────────────────
function RateControl({ sim, apiUrl }) {
  const [sliderRate, setSliderRate] = useState(10_000);
  const [applying, setApplying]     = useState(false);
  const initialized = useRef(false);

  useEffect(() => {
    if (!initialized.current && sim?.base_rate) {
      setSliderRate(Math.round(sim.base_rate));
      initialized.current = true;
    }
  }, [sim?.base_rate]);

  const applyRate = async (rate) => {
    setApplying(true);
    try {
      await fetch(`${apiUrl}/simulator/rate?rate=${rate}`, { method: "POST" });
    } finally {
      setTimeout(() => setApplying(false), 400);
    }
  };

  const effectiveRate  = Math.round(sim?.current_rate || 0);
  const maxRate        = sim?.max_rate || sliderRate * 1.2;
  const utilization    = Math.min(100, (effectiveRate / maxRate) * 100);
  const lagRatio       = Math.min(100, ((sim?.target_lag || 0) / (sim?.max_lag || 1)) * 100);

  const utilizationColor =
    utilization > 90 ? C.red
    : utilization > 65 ? C.yellow
    : C.accent;

  return (
    <div>
      <SectionHeader
        title="RATE CONTROL"
        sub="Drag to adjust throughput — watch backpressure engage when queues fill"
        color={C.orange}
      />

      <div style={{ marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
          <span style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>TARGET</span>
          <span style={{ color: C.orange, fontSize: 14, fontWeight: 800, fontFamily: "'DM Mono', monospace" }}>
            {fmt.num(sliderRate)} eps
          </span>
        </div>
        <input
          type="range" min={500} max={20_000} step={500} value={sliderRate}
          onChange={(e) => setSliderRate(Number(e.target.value))}
          onMouseUp={() => applyRate(sliderRate)}
          onTouchEnd={() => applyRate(sliderRate)}
          style={{ width: "100%", accentColor: C.orange, cursor: "pointer", height: 4 }}
        />
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
          <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>500</span>
          <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>20k eps</span>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 14 }}>
        <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 12px" }}>
          <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.1em", marginBottom: 4 }}>EFFECTIVE</div>
          <div style={{ color: C.text, fontSize: 16, fontWeight: 700, fontFamily: "'DM Mono'" }}>
            {fmt.num(effectiveRate)}
          </div>
          <div style={{ color: C.textDim, fontSize: 9 }}>events/sec</div>
        </div>
        <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 12px" }}>
          <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.1em", marginBottom: 4 }}>UTILIZATION</div>
          <div style={{ color: utilizationColor, fontSize: 16, fontWeight: 700, fontFamily: "'DM Mono'" }}>
            {utilization.toFixed(0)}%
          </div>
          <div style={{ color: C.textDim, fontSize: 9 }}>of capacity</div>
        </div>
      </div>

      <div style={{ height: 8, background: C.border, borderRadius: 4, overflow: "hidden" }}>
        <div style={{
          height: "100%", width: `${utilization}%`,
          background: `linear-gradient(90deg, ${C.accent}, ${utilizationColor})`,
          borderRadius: 4, transition: "width 0.5s ease",
          boxShadow: `0 0 10px ${utilizationColor}66`,
        }} />
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 5 }}>
        <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>0</span>
        <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>
          {applying ? "APPLYING..." : `MAX ${fmt.num(Math.round(maxRate))} eps`}
        </span>
      </div>
    </div>
  );
}

// ─── Flash Sale Banner ─────────────────────────────────────────────────────────
function FlashSaleBanner({ active }) {
  if (!active) return null;
  return (
    <div style={{
      background: `linear-gradient(90deg, ${C.orange}18, ${C.yellow}18, ${C.orange}18)`,
      border: `1px solid ${C.orange}88`,
      borderRadius: 10,
      padding: "12px 24px",
      textAlign: "center",
      animation: "pulse 1.2s ease-in-out infinite",
      fontFamily: "'Space Mono', monospace",
      fontSize: 12,
      letterSpacing: "0.1em",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      gap: 16,
    }}>
      <span style={{ color: C.orange, fontWeight: 700 }}>⚡ FLASH SALE ACTIVE</span>
      <span style={{ color: C.borderHigh }}>|</span>
      <span style={{ color: C.textMid }}>Traffic spike in progress — backpressure system engaged</span>
    </div>
  );
}

// ─── Main Dashboard ────────────────────────────────────────────────────────────
const WS_URL  = "ws://localhost:8000/ws/live";
const API_URL = "http://localhost:8000";

export default function Dashboard() {
  const { data, connected, flashSale } = useWebSocket(WS_URL);
  const [history, setHistory] = useState({ eps: [], revenue: [], latency: [] });
  const [resetting, setResetting] = useState(false);

  const metrics = data?.metrics  || {};
  const broker  = data?.broker   || [];
  const sim     = data?.simulator || {};
  const engine  = data?.engine   || {};
  const workers = data?.workers  || [];

  useEffect(() => {
    if (!data?.metrics) return;
    const now = Date.now() / 1000;
    setHistory((prev) => ({
      eps: [...prev.eps.slice(-59), { ts: now, count: metrics.avg_eps_5s || 0, raw: metrics.current_eps || 0 }],
      revenue: metrics.revenue_per_minute?.length ? metrics.revenue_per_minute : prev.revenue,
      latency: [...prev.latency.slice(-59), { ts: now, p50: metrics.p50_latency_ms || 0, p99: metrics.p99_latency_ms || 0 }],
    }));
  }, [data]);

  const triggerFlashSale = async () => {
    await fetch(`${API_URL}/simulator/flash-sale`, { method: "POST" });
  };

  const resetSystem = async () => {
    if (resetting) return;
    if (!window.confirm("Reset the engine? Clears all metrics, queues, logs, and checkpoints.")) return;
    setResetting(true);
    try {
      await fetch(`${API_URL}/system/reset`, { method: "POST" });
      setHistory({ eps: [], revenue: [], latency: [] });
    } finally {
      setResetting(false);
    }
  };

  const refundRate = metrics.total_orders > 0
    ? ((metrics.refunds / metrics.total_orders) * 100).toFixed(1)
    : "0.0";

  const dedupEffectiveness = sim.total_duplicates_injected > 0
    ? Math.min(100, Math.round((metrics.duplicates_rejected / sim.total_duplicates_injected) * 100))
    : null;

  return (
    <div style={{ background: C.bg, minHeight: "100vh", color: C.text, fontFamily: "'Space Mono', monospace", padding: "0 0 60px" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Mono:wght@300;400;500&display=swap');
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        body { background: ${C.bg}; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: ${C.bg}; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 2px; }
        input[type=range] { -webkit-appearance: none; appearance: none; height: 4px; background: ${C.border}; border-radius: 2px; outline: none; }
        input[type=range]::-webkit-slider-thumb { -webkit-appearance: none; width: 16px; height: 16px; border-radius: 50%; background: ${C.orange}; cursor: pointer; box-shadow: 0 0 10px ${C.orange}88; }
        @keyframes pulse  { 0%,100% { opacity:1 }       50% { opacity:0.7 } }
        @keyframes blink  { 0%,100% { opacity:1 }       50% { opacity:0.25 } }
        @keyframes fadeUp { from { opacity:0; transform:translateY(10px) } to { opacity:1; transform:translateY(0) } }
        .card { animation: fadeUp 0.4s ease both; }
        .card:hover { border-color: ${C.borderHigh} !important; box-shadow: 0 0 24px ${C.accent}0a; }
        .btn { transition: background 0.2s, box-shadow 0.2s; }
        .btn:hover { box-shadow: 0 0 16px currentColor44; }
      `}</style>

      {/* ── Header ───────────────────────────────────────────────────────────── */}
      <div style={{
        background: `${C.surface}ee`, backdropFilter: "blur(12px)",
        borderBottom: `1px solid ${C.border}`,
        padding: "14px 32px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        position: "sticky", top: 0, zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <div style={{
            width: 8, height: 8, borderRadius: "50%",
            background: connected ? C.accent : C.red,
            boxShadow: `0 0 10px ${connected ? C.accent : C.red}`,
            animation: connected ? "none" : "blink 1s infinite",
          }} />
          <span style={{ color: connected ? C.accent : C.red, fontSize: 10, letterSpacing: "0.2em" }}>
            {connected ? "LIVE" : "RECONNECTING"}
          </span>
          <span style={{ color: C.border }}>│</span>
          <span style={{ color: C.text, fontSize: 13, fontWeight: 700, letterSpacing: "0.08em" }}>COMMERCE STREAM</span>
          <span style={{ color: C.textDim, fontSize: 10 }}>/ REAL-TIME ANALYTICS</span>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          {engine.uptime_seconds > 0 && (
            <div style={{ textAlign: "right" }}>
              <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.12em" }}>UPTIME</div>
              <div style={{ color: C.textMid, fontSize: 11, fontWeight: 700, fontFamily: "'DM Mono'" }}>
                {fmt.uptime(engine.uptime_seconds)}
              </div>
            </div>
          )}
          {sim.total_generated > 0 && (
            <div style={{ textAlign: "right" }}>
              <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.12em" }}>EVENTS IN</div>
              <div style={{ color: C.textMid, fontSize: 11, fontWeight: 700, fontFamily: "'DM Mono'" }}>
                {fmt.num(sim.total_generated)}
              </div>
            </div>
          )}
          {flashSale && (
            <span style={{ color: C.orange, fontSize: 10, fontWeight: 700, animation: "blink 0.8s infinite", letterSpacing: "0.1em" }}>
              ⚡ FLASH SALE
            </span>
          )}
          <button className="btn" onClick={triggerFlashSale} style={{
            background: "transparent", border: `1px solid ${C.orange}88`, color: C.orange,
            padding: "6px 14px", borderRadius: 6, cursor: "pointer",
            fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.1em",
          }}>
            ⚡ FLASH SALE
          </button>
          <button className="btn" onClick={resetSystem} disabled={resetting} style={{
            background: resetting ? `${C.red}18` : "transparent",
            border: `1px solid ${C.red}88`, color: C.red,
            padding: "6px 14px", borderRadius: 6,
            cursor: resetting ? "wait" : "pointer",
            fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.1em",
            opacity: resetting ? 0.7 : 1,
          }}>
            {resetting ? "RESETTING..." : "RESET"}
          </button>
        </div>
      </div>

      <div style={{ padding: "24px 32px", display: "flex", flexDirection: "column", gap: 20 }}>

        {/* ── Flash Sale Banner ───────────────────────────────────────────────── */}
        <FlashSaleBanner active={flashSale} />

        {/* ── KPI Row ─────────────────────────────────────────────────────────── */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12 }}>
          <KPICard label="Total Revenue"  value={metrics.total_revenue  || 0} format={fmt.currency} color={C.accent}  icon="$"
            sub={`AOV ${fmt.currency(metrics.avg_order_value || 0)}`} />
          <KPICard label="Total Orders"   value={metrics.total_orders   || 0} format={fmt.num}      color={C.blue}    icon="📦"
            sub={`${metrics.refunds || 0} refunds · ${refundRate}% rate`} />
          <KPICard label="Active Users"   value={metrics.active_users   || 0} format={fmt.num}      color={C.purple}  icon="👤"
            sub="unique this session" />
          <KPICard label="Events / sec"   value={metrics.avg_eps_5s     || 0} format={(v) => `${fmt.num(v)}`} color={C.orange} icon="⚡"
            sub={`5s avg · raw ${fmt.num(metrics.current_eps || 0)}/s`} />
          <KPICard label="Latency P50"    value={metrics.p50_latency_ms || 0} format={(v) => `${v}ms`}         color={C.red}    icon="⏱"
            sub={`P99 ${metrics.p99_latency_ms || 0}ms · avg ${metrics.avg_latency_ms || 0}ms`} />
          <KPICard label="Total Processed" value={metrics.total_events  || 0} format={fmt.num}      color={C.yellow}  icon="∑"
            sub={`${broker.length} partitions · ${broker.reduce((s, p) => s + p.lag, 0)} lag`} />
        </div>

        {/* ── Charts ──────────────────────────────────────────────────────────── */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="REVENUE / MINUTE" sub="60-min rolling windowed aggregation" color={C.accent} />
            <ResponsiveContainer width="100%" height={155}>
              <AreaChart data={history.revenue}>
                <defs>
                  <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={C.accent} stopOpacity={0.25} />
                    <stop offset="95%" stopColor={C.accent} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} tickFormatter={fmt.currency} width={52} />
                <Tooltip content={<ChartTooltip rows={(p) => <div style={{ color: C.accent }}>{fmt.currency(p[0]?.value)}</div>} />} />
                <Area type="monotone" dataKey="revenue" stroke={C.accent} strokeWidth={2} fill="url(#revGrad)" dot={false} isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="EVENTS / SECOND" sub="5-second rolling average — last 60 seconds" color={C.orange} />
            <ResponsiveContainer width="100%" height={155}>
              <AreaChart data={history.eps}>
                <defs>
                  <linearGradient id="epsGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={C.orange} stopOpacity={0.25} />
                    <stop offset="95%" stopColor={C.orange} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} width={34} />
                <Tooltip content={
                  <ChartTooltip rows={(p) => (
                    <>
                      <div style={{ color: C.orange }}>avg: {fmt.num(p[0]?.value)}/s</div>
                      <div style={{ color: C.textDim }}>raw: {fmt.num(p[0]?.payload?.raw)}/s</div>
                    </>
                  )} />
                } />
                <Area type="monotone" dataKey="count" stroke={C.orange} strokeWidth={2} fill="url(#epsGrad)" dot={false} isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="END-TO-END LATENCY" sub="Produce → state store · last 60 seconds" color={C.red} />
            <ResponsiveContainer width="100%" height={155}>
              <LineChart data={history.latency}>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} width={40} tickFormatter={(v) => `${v}ms`} />
                <Tooltip content={
                  <ChartTooltip rows={(p) => (
                    <>
                      <div style={{ color: C.red }}>P50: {p[0]?.value}ms</div>
                      <div style={{ color: C.red + "88" }}>P99: {p[1]?.value}ms</div>
                    </>
                  )} />
                } />
                <Line type="monotone" dataKey="p50" stroke={C.red} strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line type="monotone" dataKey="p99" stroke={C.red + "55"} strokeWidth={1.5} strokeDasharray="4 4" dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
            <div style={{ display: "flex", gap: 16, marginTop: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                <div style={{ width: 16, height: 2, background: C.red, borderRadius: 1 }} />
                <span style={{ color: C.textDim, fontSize: 9 }}>P50</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                <div style={{ width: 16, height: 1, background: C.red + "55", borderTop: `1px dashed ${C.red}55` }} />
                <span style={{ color: C.textDim, fontSize: 9 }}>P99</span>
              </div>
            </div>
          </div>
        </div>

        {/* ── Rate Control + Shard Workers ──────────────────────────────────────── */}
        <div style={{ display: "grid", gridTemplateColumns: "340px 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <RateControl sim={sim} apiUrl={API_URL} />
          </div>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <ShardWorkers workers={workers} />
          </div>
        </div>

        {/* ── Funnel + Products + Geo/Device ────────────────────────────────────── */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="CONVERSION FUNNEL" sub="Real-time drop-off at each stage" color={C.blue} />
            <FunnelChart funnel={metrics.funnel} />
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="TOP PRODUCTS" sub="Live purchase rankings" color={C.purple} />
            <TopProducts products={metrics.top_products} />
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="GEO & DEVICE" sub="Traffic breakdown by origin and device type" color={C.yellow} />
            <GeoDevice geo={metrics.geo_distribution} deviceDist={metrics.device_distribution} />
          </div>
        </div>

        {/* ── Broker Health ─────────────────────────────────────────────────────── */}
        <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
          <SectionHeader
            title="BROKER — PARTITION HEALTH"
            sub="Kafka-inspired design · bounded queues enforce backpressure when consumers fall behind producers"
            color={C.teal}
          />
          <PartitionHealth partitions={broker} />
          <div style={{
            display: "flex", gap: 24, marginTop: 14,
            paddingTop: 14, borderTop: `1px solid ${C.border}`,
          }}>
            {[
              { label: "TOTAL PRODUCED", value: fmt.num(broker.reduce((s, p) => s + p.total_produced, 0)), color: C.accent },
              { label: "TOTAL CONSUMED", value: fmt.num(broker.reduce((s, p) => s + p.total_consumed, 0)), color: C.blue },
              { label: "QUEUE LAG", value: fmt.num(broker.reduce((s, p) => s + p.lag, 0)), color: broker.reduce((s, p) => s + p.lag, 0) > 50_000 ? C.red : C.yellow },
              { label: "BACKPRESSURE EVENTS", value: fmt.num(broker.reduce((s, p) => s + p.backpressure_events, 0)), color: C.orange },
            ].map(({ label, value, color }) => (
              <div key={label}>
                <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.12em", marginBottom: 4 }}>{label}</div>
                <div style={{ color, fontSize: 16, fontWeight: 700, fontFamily: "'DM Mono'" }}>{value}</div>
              </div>
            ))}
          </div>
        </div>

        {/* ── Exactly-Once Guarantee ────────────────────────────────────────────── */}
        <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
          <SectionHeader
            title="EXACTLY-ONCE GUARANTEE"
            sub="Simulator injects ~0.2% duplicate events (producer retries) · state store deduplicates by event ID before touching state"
            color={C.purple}
          />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
            {[
              {
                label: "DUPLICATES INJECTED",
                value: <AnimatedValue value={sim.total_duplicates_injected || 0} format={fmt.num} color={C.orange} />,
                sub: "simulated producer retries",
                color: C.orange,
              },
              {
                label: "DUPLICATES REJECTED",
                value: <AnimatedValue value={metrics.duplicates_rejected || 0} format={fmt.num} color={C.accent} />,
                sub: "caught before state mutation",
                color: C.accent,
              },
              {
                label: "DEDUP EFFECTIVENESS",
                value: <span style={{ color: C.purple, fontWeight: 800 }}>
                  {dedupEffectiveness !== null ? `${dedupEffectiveness}%` : "—"}
                </span>,
                sub: "injected duplicates caught",
                color: C.purple,
              },
              {
                label: "DUPLICATE RATE",
                value: <span style={{ color: C.yellow, fontWeight: 800 }}>
                  {((sim.duplicate_rate || 0) * 100).toFixed(1)}%
                </span>,
                sub: "of produced events are retries",
                color: C.yellow,
              },
            ].map(({ label, value, sub, color }) => (
              <div key={label} style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "14px 16px" }}>
                <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.12em", marginBottom: 8 }}>{label}</div>
                <div style={{ fontSize: 22, fontWeight: 800, fontFamily: "'DM Mono', monospace", lineHeight: 1 }}>{value}</div>
                <div style={{ color: C.textDim, fontSize: 10, marginTop: 8 }}>{sub}</div>
              </div>
            ))}
          </div>
        </div>

        {/* ── Search Terms + Category Performance ───────────────────────────────── */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="TOP SEARCH TERMS" sub="Real-time search stream" color={C.teal} />
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {(metrics.search_terms || []).map(([term, count]) => (
                <div key={term} style={{
                  background: C.accentDim, border: `1px solid ${C.accent}30`,
                  borderRadius: 20, padding: "5px 13px",
                  fontSize: 11, color: C.accent, fontFamily: "'Space Mono', monospace",
                }}>
                  {term} <span style={{ color: C.textDim }}>({fmt.num(count)})</span>
                </div>
              ))}
            </div>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="CATEGORY PERFORMANCE" sub="Purchase count by product category" color={C.yellow} />
            <ResponsiveContainer width="100%" height={155}>
              <BarChart data={(metrics.top_categories || []).map(([name, value]) => ({ name: name.split(" ")[0], value }))}>
                <XAxis dataKey="name" tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis hide />
                <Tooltip
                  contentStyle={{ background: C.surfaceHigh, border: `1px solid ${C.borderHigh}`, borderRadius: 8, fontFamily: "'Space Mono'", fontSize: 11 }}
                  labelStyle={{ color: C.text }} itemStyle={{ color: C.yellow }}
                />
                <Bar dataKey="value" radius={[4, 4, 0, 0]} isAnimationActive={false}>
                  {(metrics.top_categories || []).map((_, i) => (
                    <Cell key={i} fill={[C.accent, C.blue, C.purple, C.orange, C.yellow, C.teal, C.red, C.accent][i % 8]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* ── Footer ───────────────────────────────────────────────────────────── */}
        <div style={{
          textAlign: "center", color: C.textDim, fontSize: 9,
          fontFamily: "'Space Mono', monospace", letterSpacing: "0.18em",
          borderTop: `1px solid ${C.border}`, paddingTop: 20, lineHeight: 2,
        }}>
          COMMERCE STREAM ENGINE · {broker.length} PARTITIONS · {workers.length} SHARD WORKERS · WAL-BACKED STATE · EXACTLY-ONCE DELIVERY
          <br />
          {fmt.num(metrics.total_events || 0)} EVENTS PROCESSED · {fmt.num(sim.total_generated || 0)} EVENTS GENERATED · UPTIME {fmt.uptime(engine.uptime_seconds)}
        </div>

      </div>
    </div>
  );
}
