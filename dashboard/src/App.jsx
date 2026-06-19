import { useState, useEffect, useRef, useCallback } from "react";
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Cell
} from "recharts";

// ─── Design System ────────────────────────────────────────────────────────────
const C = {
  bg: "#0a0a0f",
  surface: "#10101a",
  border: "#1e1e30",
  accent: "#00ff9d",
  accentDim: "#00ff9d22",
  orange: "#ff6b2b",
  orangeDim: "#ff6b2b22",
  blue: "#4d9fff",
  blueDim: "#4d9fff22",
  purple: "#b06bff",
  purpleDim: "#b06bff22",
  yellow: "#ffd060",
  text: "#e8e8f0",
  textDim: "#6b6b8a",
  red: "#ff4560",
};

const fmt = {
  currency: (v) => v >= 1000 ? `$${(v/1000).toFixed(1)}k` : `$${(v||0).toFixed(2)}`,
  num: (v) => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : v >= 1000 ? `${(v/1000).toFixed(1)}k` : (v||0).toLocaleString(),
  ts: (v) => new Date(v * 1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}),
};

// ─── Hooks ────────────────────────────────────────────────────────────────────
function useWebSocket(url) {
  const [data, setData] = useState(null);
  const [connected, setConnected] = useState(false);
  const [flashSale, setFlashSale] = useState(false);
  const ws = useRef(null);
  const prevRevenue = useRef(0);

  useEffect(() => {
    const connect = () => {
      ws.current = new WebSocket(url);
      ws.current.onopen = () => setConnected(true);
      ws.current.onclose = () => { setConnected(false); setTimeout(connect, 2000); };
      ws.current.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        setData(msg);
        if (msg.simulator?.flash_sale_active !== undefined) {
          setFlashSale(msg.simulator.flash_sale_active);
        }
      };
    };
    connect();
    return () => ws.current?.close();
  }, [url]);

  return { data, connected, flashSale };
}

// ─── Animated Counter ────────────────────────────────────────────────────────
function AnimatedValue({ value, format, color = C.text }) {
  const [display, setDisplay] = useState(value);
  const [bump, setBump] = useState(false);
  const prev = useRef(value);

  useEffect(() => {
    if (value !== prev.current) {
      setBump(true);
      setTimeout(() => setBump(false), 300);
      prev.current = value;
    }
    setDisplay(value);
  }, [value]);

  return (
    <span style={{
      color,
      transition: "all 0.3s ease",
      transform: bump ? "scale(1.05)" : "scale(1)",
      display: "inline-block",
      textShadow: bump ? `0 0 20px ${color}88` : "none",
    }}>
      {format(display)}
    </span>
  );
}

// ─── KPI Card ────────────────────────────────────────────────────────────────
function KPICard({ label, value, format, color, icon, sub }) {
  return (
    <div style={{
      background: C.surface,
      border: `1px solid ${C.border}`,
      borderRadius: 12,
      padding: "20px 24px",
      position: "relative",
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, height: 2,
        background: color, opacity: 0.8,
      }} />
      <div style={{ color: C.textDim, fontSize: 11, fontFamily: "'Space Mono', monospace", letterSpacing: "0.15em", textTransform: "uppercase", marginBottom: 8 }}>
        {icon} {label}
      </div>
      <div style={{ fontSize: 28, fontWeight: 800, fontFamily: "'DM Mono', monospace", letterSpacing: "-0.02em" }}>
        <AnimatedValue value={value} format={format} color={color} />
      </div>
      {sub && <div style={{ color: C.textDim, fontSize: 12, marginTop: 6, fontFamily: "'Space Mono', monospace" }}>{sub}</div>}
    </div>
  );
}

// ─── Partition Health ─────────────────────────────────────────────────────────
function PartitionHealth({ partitions }) {
  if (!partitions?.length) return null;
  return (
    <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
      {partitions.map(p => {
        const lagPct = Math.min(100, (p.lag / 10000) * 100);
        const color = lagPct > 70 ? C.red : lagPct > 30 ? C.yellow : C.accent;
        return (
          <div key={p.partition_id} style={{
            background: C.surface, border: `1px solid ${C.border}`,
            borderRadius: 8, padding: "10px 14px", minWidth: 130, flex: 1,
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
              <span style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", letterSpacing: "0.1em" }}>
                PARTITION {p.partition_id}
              </span>
              <span style={{ color, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                {p.lag} lag
              </span>
            </div>
            <div style={{ height: 4, background: C.border, borderRadius: 2, overflow: "hidden" }}>
              <div style={{
                height: "100%", width: `${lagPct}%`,
                background: color, borderRadius: 2,
                transition: "width 0.5s ease, background 0.3s",
                boxShadow: `0 0 8px ${color}88`,
              }} />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 5 }}>
              <span style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                {fmt.num(p.total_produced)} produced
              </span>
              <span style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                {fmt.num(p.total_consumed)} consumed
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Funnel Chart ─────────────────────────────────────────────────────────────
function FunnelChart({ funnel }) {
  if (!funnel) return null;
  const stages = [
    { key: "page_view", label: "Page Views", color: C.blue },
    { key: "product_click", label: "Clicks", color: C.purple },
    { key: "add_to_cart", label: "Add to Cart", color: C.yellow },
    { key: "checkout_start", label: "Checkout", color: C.orange },
    { key: "purchase", label: "Purchase", color: C.accent },
  ];
  const max = Math.max(...stages.map(s => funnel[s.key] || 0), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {stages.map((stage, i) => {
        const val = funnel[stage.key] || 0;
        const pct = (val / max) * 100;
        const convRate = i > 0 ? ((val / (funnel[stages[i-1].key] || 1)) * 100).toFixed(1) : null;
        return (
          <div key={stage.key}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ color: C.textDim, fontSize: 11, fontFamily: "'Space Mono', monospace" }}>
                {stage.label}
              </span>
              <div style={{ display: "flex", gap: 12 }}>
                {convRate && (
                  <span style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                    ↓ {convRate}%
                  </span>
                )}
                <span style={{ color: stage.color, fontSize: 11, fontFamily: "'DM Mono', monospace", fontWeight: 700 }}>
                  {fmt.num(val)}
                </span>
              </div>
            </div>
            <div style={{ height: 20, background: C.border, borderRadius: 4, overflow: "hidden", position: "relative" }}>
              <div style={{
                height: "100%", width: `${pct}%`,
                background: stage.color,
                borderRadius: 4,
                transition: "width 0.6s cubic-bezier(0.4,0,0.2,1)",
                boxShadow: `0 0 12px ${stage.color}44`,
                opacity: 0.85,
              }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Top Products Bar ─────────────────────────────────────────────────────────
function TopProducts({ products }) {
  if (!products?.length) return null;
  const max = products[0]?.[1] || 1;
  const colors = [C.accent, C.blue, C.purple, C.orange, C.yellow, C.accent, C.blue, C.purple, C.orange, C.yellow];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
      {products.slice(0, 8).map(([name, count], i) => (
        <div key={name}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
            <span style={{ color: C.textDim, fontSize: 11, fontFamily: "'Space Mono', monospace",
              whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: "70%" }}>
              {name}
            </span>
            <span style={{ color: colors[i], fontSize: 11, fontFamily: "'DM Mono', monospace", fontWeight: 700 }}>
              {count}
            </span>
          </div>
          <div style={{ height: 6, background: C.border, borderRadius: 3, overflow: "hidden" }}>
            <div style={{
              height: "100%", width: `${(count / max) * 100}%`,
              background: colors[i], borderRadius: 3,
              transition: "width 0.5s ease",
              boxShadow: `0 0 8px ${colors[i]}44`,
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Geo Distribution ────────────────────────────────────────────────────────
function GeoChart({ geo }) {
  if (!geo?.length) return null;
  const total = geo.reduce((s, [,v]) => s + v, 0) || 1;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {geo.slice(0, 6).map(([country, count], i) => {
        const pct = ((count / total) * 100).toFixed(1);
        const colors = [C.accent, C.blue, C.purple, C.orange, C.yellow, C.red];
        return (
          <div key={country} style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ color: C.textDim, fontSize: 11, fontFamily: "'Space Mono', monospace", width: 28 }}>
              {country}
            </span>
            <div style={{ flex: 1, height: 8, background: C.border, borderRadius: 4, overflow: "hidden" }}>
              <div style={{
                height: "100%", width: `${pct}%`,
                background: colors[i], borderRadius: 4,
                transition: "width 0.6s ease",
              }} />
            </div>
            <span style={{ color: colors[i], fontSize: 11, fontFamily: "'DM Mono', monospace", width: 42, textAlign: "right" }}>
              {pct}%
            </span>
          </div>
        );
      })}
    </div>
  );
}

// ─── Flash Sale Banner ────────────────────────────────────────────────────────
function FlashSaleBanner({ active }) {
  if (!active) return null;
  return (
    <div style={{
      background: `linear-gradient(90deg, ${C.orange}22, ${C.yellow}22, ${C.orange}22)`,
      border: `1px solid ${C.orange}`,
      borderRadius: 8,
      padding: "10px 20px",
      textAlign: "center",
      animation: "pulse 1s ease-in-out infinite",
      fontFamily: "'Space Mono', monospace",
      fontSize: 13,
      letterSpacing: "0.1em",
    }}>
      <span style={{ color: C.orange, fontWeight: 700 }}>⚡ FLASH SALE ACTIVE</span>
      <span style={{ color: C.textDim, marginLeft: 12 }}>Backpressure system engaged — handling traffic spike</span>
    </div>
  );
}

// ─── Section Header ───────────────────────────────────────────────────────────
function SectionHeader({ title, sub }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ color: C.text, fontSize: 13, fontWeight: 700, fontFamily: "'Space Mono', monospace", letterSpacing: "0.08em" }}>
        {title}
      </div>
      {sub && <div style={{ color: C.textDim, fontSize: 10, fontFamily: "'Space Mono', monospace", marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

// ─── Custom Tooltip ───────────────────────────────────────────────────────────
function CustomTooltip({ active, payload, label, valueFormat = (v) => v }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: C.surface, border: `1px solid ${C.border}`,
      borderRadius: 8, padding: "8px 12px", fontFamily: "'Space Mono', monospace", fontSize: 11,
    }}>
      <div style={{ color: C.textDim, marginBottom: 4 }}>{fmt.ts(label)}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || C.accent }}>
          {valueFormat(p.value)}
        </div>
      ))}
    </div>
  );
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────
const WS_URL = "ws://localhost:8000/ws/live";
const API_URL = "http://localhost:8000";

export default function Dashboard() {
  const { data, connected, flashSale } = useWebSocket(WS_URL);
  const [history, setHistory] = useState({ eps: [], revenue: [], latency: [] });
  const [resetting, setResetting] = useState(false);

  const metrics = data?.metrics || {};
  const broker = data?.broker || [];
  const sim = data?.simulator || {};
  const engine = data?.engine || {};

  // Maintain rolling history for time-series charts
  useEffect(() => {
    if (!data?.metrics) return;
    const now = Date.now() / 1000;
    setHistory(prev => ({
      eps: [...prev.eps.slice(-59), {
        ts: now,
        count: metrics.avg_eps_5s || 0,
        raw: metrics.current_eps || 0,
      }],
      revenue: metrics.revenue_per_minute?.length ? metrics.revenue_per_minute : prev.revenue,
      latency: [...prev.latency.slice(-59), {
        ts: now,
        p50: metrics.p50_latency_ms || 0,
        p99: metrics.p99_latency_ms || 0,
      }],
    }));
  }, [data]);

  const triggerFlashSale = async () => {
    await fetch(`${API_URL}/simulator/flash-sale`, { method: "POST" });
  };

  const resetSystem = async () => {
    if (resetting) return;
    const confirmed = window.confirm("Fully reset the engine? This clears live metrics, queues, logs, and checkpoints.");
    if (!confirmed) return;

    setResetting(true);
    try {
      await fetch(`${API_URL}/system/reset`, { method: "POST" });
      setHistory({ eps: [], revenue: [], latency: [] });
    } finally {
      setResetting(false);
    }
  };

  const deviceData = Object.entries(metrics.device_distribution || {}).map(([name, value]) => ({ name, value }));

  return (
    <div style={{
      background: C.bg,
      minHeight: "100vh",
      color: C.text,
      fontFamily: "'Space Mono', monospace",
      padding: "0 0 40px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Mono:wght@300;400;500&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 4px; } ::-webkit-scrollbar-track { background: ${C.bg}; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 2px; }
        @keyframes pulse { 0%,100% { opacity:1 } 50% { opacity:0.7 } }
        @keyframes blink { 0%,100% { opacity:1 } 50% { opacity:0.3 } }
        @keyframes slideIn { from { opacity:0; transform:translateY(-8px) } to { opacity:1; transform:translateY(0) } }
        .card { animation: slideIn 0.4s ease both; }
      `}</style>

      {/* Header */}
      <div style={{
        background: C.surface,
        borderBottom: `1px solid ${C.border}`,
        padding: "16px 32px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        position: "sticky",
        top: 0,
        zIndex: 100,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{
            width: 8, height: 8, borderRadius: "50%",
            background: connected ? C.accent : C.red,
            boxShadow: `0 0 10px ${connected ? C.accent : C.red}`,
            animation: connected ? "none" : "blink 1s infinite",
          }} />
          <span style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.2em" }}>
            {connected ? "LIVE" : "CONNECTING..."}
          </span>
          <span style={{ color: C.border }}>|</span>
          <span style={{ color: C.text, fontSize: 13, fontWeight: 700, letterSpacing: "0.1em" }}>
            STREAM ENGINE
          </span>
          <span style={{ color: C.textDim, fontSize: 10 }}>/ E-COMMERCE ANALYTICS</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{ color: C.textDim, fontSize: 10 }}>
            {sim.total_generated ? `${fmt.num(sim.total_generated)} events generated` : ""}
          </div>
          {flashSale && (
            <div style={{ color: C.orange, fontSize: 10, animation: "blink 0.8s infinite", fontWeight: 700 }}>
              ⚡ FLASH SALE
            </div>
          )}
          <button onClick={triggerFlashSale} style={{
            background: "transparent",
            border: `1px solid ${C.orange}`,
            color: C.orange,
            padding: "6px 14px",
            borderRadius: 6,
            cursor: "pointer",
            fontSize: 10,
            fontFamily: "'Space Mono', monospace",
            letterSpacing: "0.1em",
            transition: "all 0.2s",
          }}
            onMouseEnter={e => e.target.style.background = C.orangeDim}
            onMouseLeave={e => e.target.style.background = "transparent"}
          >
            ⚡ TRIGGER FLASH SALE
          </button>
          <button onClick={resetSystem} disabled={resetting} style={{
            background: resetting ? C.red + "22" : "transparent",
            border: `1px solid ${C.red}`,
            color: C.red,
            padding: "6px 14px",
            borderRadius: 6,
            cursor: resetting ? "wait" : "pointer",
            fontSize: 10,
            fontFamily: "'Space Mono', monospace",
            letterSpacing: "0.1em",
            transition: "all 0.2s",
            opacity: resetting ? 0.7 : 1,
          }}
            onMouseEnter={e => { if (!resetting) e.target.style.background = `${C.red}22`; }}
            onMouseLeave={e => { if (!resetting) e.target.style.background = resetting ? `${C.red}22` : "transparent"; }}
          >
            {resetting ? "RESETTING..." : "RESET SYSTEM"}
          </button>
        </div>
      </div>

      <div style={{ padding: "24px 32px", display: "flex", flexDirection: "column", gap: 20 }}>

        {/* Flash Sale Banner */}
        <FlashSaleBanner active={flashSale} />

        {/* KPI Row */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12 }}>
          <KPICard label="Total Revenue" value={metrics.total_revenue || 0} format={fmt.currency} color={C.accent} icon="$"
            sub={`AOV ${fmt.currency(metrics.avg_order_value || 0)}`} />
          <KPICard label="Total Orders" value={metrics.total_orders || 0} format={fmt.num} color={C.blue} icon="📦"
            sub={`${metrics.refunds || 0} refunds`} />
          <KPICard label="Active Users" value={metrics.active_users || 0} format={fmt.num} color={C.purple} icon="👤"
            sub="unique this session" />
          <KPICard label="Events/sec" value={metrics.avg_eps_5s || 0} format={v => `${fmt.num(v)} eps`} color={C.orange} icon="⚡"
            sub={`raw ${fmt.num(metrics.current_eps || 0)} eps`} />
          <KPICard label="Latency P50" value={metrics.p50_latency_ms || 0} format={v => `${v}ms`} color={C.red} icon="⏱"
            sub={`P99 ${metrics.p99_latency_ms || 0}ms`} />
          <KPICard label="Partitions" value={broker.length || 0} format={v => `${v} active`} color={C.yellow} icon="⚙"
            sub={`${broker.reduce((s,p) => s + p.lag, 0)} total lag`} />
        </div>

        <div className="card" style={{
          background: C.surface,
          border: `1px solid ${C.border}`,
          borderRadius: 12,
          padding: "14px 18px",
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 12,
        }}>
          <div>
            <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em" }}>MODE</div>
            <div style={{ color: C.accent, fontSize: 16, fontWeight: 700 }}>{(engine.mode || "demo").toUpperCase()}</div>
          </div>
          <div>
            <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em" }}>SHARDS</div>
            <div style={{ color: C.text, fontSize: 16, fontWeight: 700 }}>{fmt.num(engine.shard_count || 0)}</div>
          </div>
          <div>
            <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em" }}>BATCH SIZE</div>
            <div style={{ color: C.text, fontSize: 16, fontWeight: 700 }}>{fmt.num(engine.batch_size || 0)}</div>
          </div>
          <div>
            <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em" }}>UPTIME</div>
            <div style={{ color: C.text, fontSize: 16, fontWeight: 700 }}>{Math.round(engine.uptime_seconds || 0)}s</div>
          </div>
        </div>

        {/* Revenue + EPS + Latency Charts */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="REVENUE / MINUTE" sub="Windowed aggregation — 60-min rolling" />
            <ResponsiveContainer width="100%" height={160}>
              <AreaChart data={history.revenue}>
                <defs>
                  <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={C.accent} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={C.accent} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} tickFormatter={fmt.currency} width={55} />
                <Tooltip content={<CustomTooltip valueFormat={fmt.currency} />} />
                <Area type="monotone" dataKey="revenue" stroke={C.accent} strokeWidth={2}
                  fill="url(#revGrad)" dot={false} isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="EVENTS / SECOND" sub="5-second rolling average — last 60 seconds" />
            <ResponsiveContainer width="100%" height={160}>
              <AreaChart data={history.eps}>
                <defs>
                  <linearGradient id="epsGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={C.orange} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={C.orange} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} width={35} />
                <Tooltip
                  content={({ active, payload, label }) => {
                    if (!active || !payload?.length) return null;
                    return (
                      <div style={{
                        background: C.surface, border: `1px solid ${C.border}`,
                        borderRadius: 8, padding: "8px 12px", fontFamily: "'Space Mono', monospace", fontSize: 11,
                      }}>
                        <div style={{ color: C.textDim, marginBottom: 4 }}>{fmt.ts(label)}</div>
                        <div style={{ color: C.orange }}>avg: {fmt.num(payload[0]?.value || 0)} eps</div>
                        <div style={{ color: C.textDim }}>raw: {fmt.num(payload[0]?.payload?.raw || 0)} eps</div>
                      </div>
                    );
                  }}
                />
                <Area type="monotone" dataKey="count" stroke={C.orange} strokeWidth={2}
                  fill="url(#epsGrad)" dot={false} isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="END-TO-END LATENCY" sub="Event creation → state store — last 60 seconds" />
            <ResponsiveContainer width="100%" height={160}>
              <LineChart data={history.latency}>
                <XAxis dataKey="ts" tickFormatter={fmt.ts} tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} width={42}
                  tickFormatter={v => `${v}ms`} />
                <Tooltip
                  content={({ active, payload, label }) => {
                    if (!active || !payload?.length) return null;
                    return (
                      <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "8px 12px", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                        <div style={{ color: C.textDim, marginBottom: 4 }}>{fmt.ts(label)}</div>
                        <div style={{ color: C.red }}>P50: {payload[0]?.value}ms</div>
                        <div style={{ color: C.red + "88" }}>P99: {payload[1]?.value}ms</div>
                      </div>
                    );
                  }}
                />
                <Line type="monotone" dataKey="p50" stroke={C.red} strokeWidth={2}
                  dot={false} isAnimationActive={false} name="P50" />
                <Line type="monotone" dataKey="p99" stroke={C.red + "55"} strokeWidth={1.5}
                  strokeDasharray="4 4" dot={false} isAnimationActive={false} name="P99" />
              </LineChart>
            </ResponsiveContainer>
            <div style={{ display: "flex", gap: 16, marginTop: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ width: 20, height: 2, background: C.red, borderRadius: 1 }} />
                <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>P50</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ width: 20, height: 1, background: C.red + "55", borderRadius: 1, borderTop: `1px dashed ${C.red}55` }} />
                <span style={{ color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>P99</span>
              </div>
              <div style={{ marginLeft: "auto", color: C.textDim, fontSize: 9, fontFamily: "'Space Mono', monospace" }}>
                avg {metrics.avg_latency_ms || 0}ms
              </div>
            </div>
          </div>
        </div>

        {/* Funnel + Top Products + Geo */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="CONVERSION FUNNEL" sub="Real-time drop-off tracking" />
            <FunnelChart funnel={metrics.funnel} />
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="TOP PRODUCTS" sub="Live purchase rankings" />
            <TopProducts products={metrics.top_products} />
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="GEO DISTRIBUTION" sub="Traffic by country" />
            <GeoChart geo={metrics.geo_distribution} />
            <div style={{ marginTop: 16 }}>
              <SectionHeader title="DEVICES" />
              <div style={{ display: "flex", gap: 8 }}>
                {deviceData.map((d, i) => {
                  const total = deviceData.reduce((s, x) => s + x.value, 0) || 1;
                  const pct = ((d.value / total) * 100).toFixed(0);
                  const colors = [C.blue, C.accent, C.purple];
                  return (
                    <div key={d.name} style={{ flex: 1, textAlign: "center" }}>
                      <div style={{
                        height: 40, background: C.border, borderRadius: 4, overflow: "hidden",
                        display: "flex", alignItems: "flex-end",
                      }}>
                        <div style={{
                          width: "100%", height: `${pct}%`, minHeight: 4,
                          background: colors[i], transition: "height 0.5s ease",
                        }} />
                      </div>
                      <div style={{ color: colors[i], fontSize: 11, fontWeight: 700, marginTop: 4 }}>{pct}%</div>
                      <div style={{ color: C.textDim, fontSize: 9, marginTop: 2 }}>{d.name}</div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {/* Broker Health */}
        <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
          <SectionHeader
            title="BROKER HEALTH — PARTITION LAG"
            sub="Inspired by Apache Kafka internals — bounded queues enforce backpressure when consumers fall behind"
          />
          <PartitionHealth partitions={broker} />
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8, marginTop: 12 }}>
            {broker.map(p => (
              <div key={p.partition_id} style={{
                background: C.bg, border: `1px solid ${C.border}`, borderRadius: 6,
                padding: "8px 12px",
              }}>
                <div style={{ color: C.textDim, fontSize: 9, letterSpacing: "0.1em", marginBottom: 4 }}>
                  P{p.partition_id} BACKPRESSURE EVENTS
                </div>
                <div style={{ color: p.backpressure_events > 0 ? C.orange : C.textDim, fontWeight: 700, fontSize: 13, fontFamily: "'DM Mono'" }}>
                  {fmt.num(p.backpressure_events)}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Exactly-Once Guarantee */}
        <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
          <SectionHeader
            title="EXACTLY-ONCE GUARANTEE"
            sub="Simulator injects ~0.2% duplicate events (producer retries) — state store deduplicates by event ID before touching state"
          />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
            <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "14px 18px" }}>
              <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em", marginBottom: 6 }}>DUPLICATES INJECTED</div>
              <div style={{ color: C.orange, fontSize: 22, fontWeight: 800, fontFamily: "'DM Mono', monospace" }}>
                <AnimatedValue value={sim.total_duplicates_injected || 0} format={fmt.num} color={C.orange} />
              </div>
              <div style={{ color: C.textDim, fontSize: 10, marginTop: 4 }}>simulated producer retries</div>
            </div>
            <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "14px 18px" }}>
              <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em", marginBottom: 6 }}>DUPLICATES REJECTED</div>
              <div style={{ color: C.accent, fontSize: 22, fontWeight: 800, fontFamily: "'DM Mono', monospace" }}>
                <AnimatedValue value={metrics.duplicates_rejected || 0} format={fmt.num} color={C.accent} />
              </div>
              <div style={{ color: C.textDim, fontSize: 10, marginTop: 4 }}>caught before state mutation</div>
            </div>
            <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "14px 18px" }}>
              <div style={{ color: C.textDim, fontSize: 10, letterSpacing: "0.12em", marginBottom: 6 }}>DEDUP EFFECTIVENESS</div>
              <div style={{ color: C.purple, fontSize: 22, fontWeight: 800, fontFamily: "'DM Mono', monospace" }}>
                {sim.total_duplicates_injected > 0
                  ? `${Math.min(100, Math.round((metrics.duplicates_rejected / sim.total_duplicates_injected) * 100))}%`
                  : "—"}
              </div>
              <div style={{ color: C.textDim, fontSize: 10, marginTop: 4 }}>injected duplicates caught</div>
            </div>
          </div>
        </div>

        {/* Search Terms + Categories */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="TOP SEARCH TERMS" sub="Real-time search stream" />
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {(metrics.search_terms || []).map(([term, count], i) => (
                <div key={term} style={{
                  background: C.accentDim,
                  border: `1px solid ${C.accent}44`,
                  borderRadius: 20,
                  padding: "4px 12px",
                  fontSize: 11,
                  color: C.accent,
                  fontFamily: "'Space Mono', monospace",
                }}>
                  {term} <span style={{ color: C.textDim }}>({count})</span>
                </div>
              ))}
            </div>
          </div>

          <div className="card" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, padding: 20 }}>
            <SectionHeader title="CATEGORY PERFORMANCE" sub="Revenue by product category" />
            <ResponsiveContainer width="100%" height={140}>
              <BarChart data={(metrics.top_categories || []).map(([name, value]) => ({ name: name.split(' ')[0], value }))}>
                <XAxis dataKey="name" tick={{ fill: C.textDim, fontSize: 9, fontFamily: "'Space Mono'" }} />
                <YAxis hide />
                <Tooltip
                  contentStyle={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, fontFamily: "'Space Mono'", fontSize: 11 }}
                  labelStyle={{ color: C.text }}
                  itemStyle={{ color: C.purple }}
                />
                <Bar dataKey="value" radius={[4, 4, 0, 0]} isAnimationActive={false}>
                  {(metrics.top_categories || []).map((_, i) => (
                    <Cell key={i} fill={[C.accent, C.blue, C.purple, C.orange, C.yellow, C.red, C.accent, C.blue][i % 8]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Footer */}
        <div style={{
          textAlign: "center", color: C.textDim, fontSize: 10,
          fontFamily: "'Space Mono', monospace", letterSpacing: "0.15em",
          borderTop: `1px solid ${C.border}`, paddingTop: 16,
        }}>
          STREAM ENGINE · {broker.length} PARTITIONS · WAL-BACKED STATE · POISSON EVENT SIMULATION ·
          {" "}{fmt.num(metrics.total_events || 0)} EVENTS PROCESSED
        </div>
      </div>
    </div>
  );
}
