import React, { useState, useEffect, useRef } from 'react';
import { 
  ArrowRight, Activity, Terminal, Database, 
  Zap, Play, Layout, Server 
} from 'lucide-react';
import { 
  BarChart, Bar, XAxis, Tooltip, ResponsiveContainer, Cell 
} from 'recharts';

// --- CONFIGURATION ---
// When developing locally: "http://localhost:8000"
// When deployed: "https://your-app-name.onrender.com"
const API_BASE = "http://localhost:8000"; 

export default function App() {
  const [longUrl, setLongUrl] = useState('');
  const [generatedLinks, setGeneratedLinks] = useState([]);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [analytics, setAnalytics] = useState([]);
  const logsEndRef = useRef(null);

  // Auto-scroll logs
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const addLog = (message, type = 'info', latency = 0) => {
    setLogs(prev => [...prev, { 
      message, type, latency, 
      timestamp: new Date().toLocaleTimeString().split(' ')[0] 
    }]);
  };

  // --- API: Fetch Data ---
  const fetchData = async () => {
    try {
      // Fetch URLs
      const urlsRes = await fetch(`${API_BASE}/api/urls`);
      if (urlsRes.ok) {
        const data = await urlsRes.json();
        setGeneratedLinks(data);
      }

      // Fetch Analytics
      const statsRes = await fetch(`${API_BASE}/api/analytics`);
      if (statsRes.ok) {
        const data = await statsRes.json();
        setAnalytics(data);
      }
    } catch (err) {
      console.error("Failed to fetch data (Backend might be asleep)", err);
    }
  };

  // Initial Load & Polling
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // Poll every 5s for updates
    return () => clearInterval(interval);
  }, []);

  // --- API: Shorten ---
  const handleShorten = async (e) => {
    e.preventDefault();
    if (!longUrl) return;
    setLoading(true);
    const start = performance.now();

    try {
      addLog("POST /shorten", "system");
      
      const res = await fetch(`${API_BASE}/shorten`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: longUrl })
      });

      if (!res.ok) throw new Error("Server Error");
      
      const data = await res.json();
      const latency = Math.round(performance.now() - start);
      
      addLog(`Created: ${data.short_code}`, "success", latency);
      setLongUrl('');
      fetchData(); // Refresh list immediately

    } catch (error) {
      addLog(`Error: ${error.message}`, "error");
    } finally {
      setLoading(false);
    }
  };

  // --- API: Visit (Test Redirect) ---
  const handleSimulateVisit = async (shortCode) => {
    const start = performance.now();
    addLog(`GET /${shortCode}`, "system");

    try {
      const res = await fetch(`${API_BASE}/${shortCode}`);
      const data = await res.json();
      
      const latency = Math.round(performance.now() - start);
      
      // In the backend, we return { location: "..." }
      if (data.location) {
        addLog(`Redirect -> ${data.location}`, "success", latency);
        fetchData(); // Update stats
      } else {
         addLog(`Error: ${data.detail}`, "error");
      }

    } catch (err) {
      addLog("Network Error", "error");
    }
  };

  return (
    <div className="h-screen bg-[#050505] text-[#e5e5e5] overflow-hidden flex flex-col font-sans">
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;500;600&family=Playfair+Display:ital,wght@1,400&display=swap');
        .font-mono { font-family: 'JetBrains Mono', monospace; }
        .font-sans { font-family: 'Inter', sans-serif; }
        .font-serif { font-family: 'Playfair Display', serif; }
      `}</style>

      {/* Header */}
      <header className="h-14 border-b border-[#222] flex items-center justify-between px-4 bg-[#0a0a0a]">
        <div className="flex items-center gap-4">
          <span className="font-serif italic text-lg tracking-wide pr-4 border-r border-[#222]">Savar</span>
          <div className="flex items-center gap-2 text-xs font-mono text-neutral-500">
            <Layout className="w-3 h-3" />
            <span>PRODUCTION</span>
            <span className="text-neutral-700">/</span>
            <span className="text-white">DASHBOARD</span>
          </div>
        </div>
        <div className="flex items-center gap-2 px-3 py-1 rounded bg-[#111] border border-[#222]">
           <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
           <span className="text-[10px] font-mono text-neutral-400 uppercase">Live Connection</span>
        </div>
      </header>

      {/* Main Workspace */}
      <div className="flex-1 flex overflow-hidden">
        
        {/* LEFT PANEL */}
        <div className="w-[450px] border-r border-[#222] flex flex-col bg-[#080808]">
          <div className="p-6 border-b border-[#222]">
            <h2 className="text-xs font-mono text-neutral-500 uppercase tracking-widest mb-4 flex items-center gap-2">
              <Zap className="w-3 h-3" /> Generator
            </h2>
            <form onSubmit={handleShorten} className="space-y-4">
              <input 
                type="url" required placeholder="https://..."
                className="w-full bg-[#0f0f0f] border border-[#222] rounded p-3 text-sm font-mono text-white focus:border-blue-900 focus:outline-none"
                value={longUrl} onChange={(e) => setLongUrl(e.target.value)}
              />
              <button disabled={loading} className="w-full bg-white text-black hover:bg-neutral-200 disabled:opacity-50 text-xs font-bold uppercase tracking-widest py-3 rounded flex items-center justify-center gap-2">
                {loading ? "Processing..." : "Shorten URL"} <ArrowRight className="w-3 h-3" />
              </button>
            </form>
          </div>

          <div className="flex-1 flex flex-col overflow-hidden">
             <div className="px-6 py-3 border-b border-[#222] bg-[#0a0a0a] flex justify-between items-center">
                <h2 className="text-xs font-mono text-neutral-500 uppercase flex items-center gap-2"><Database className="w-3 h-3" /> Records</h2>
             </div>
             <div className="flex-1 overflow-y-auto p-2 space-y-1">
                {generatedLinks.map((link) => (
                  <div key={link.shortCode} className="group p-3 rounded border border-transparent hover:border-[#333] hover:bg-[#111] cursor-default">
                    <div className="flex justify-between items-center mb-1">
                      <div className="flex items-center gap-2">
                        <span className="text-blue-400 font-mono text-sm">/{link.shortCode}</span>
                        <span className="text-[10px] text-neutral-600 font-mono">Clicks: {link.clicks}</span>
                      </div>
                      <button onClick={() => handleSimulateVisit(link.shortCode)} className="opacity-0 group-hover:opacity-100 bg-[#222] text-[10px] px-2 py-1 rounded text-white font-mono flex items-center gap-1">
                        TEST <Play className="w-2 h-2" />
                      </button>
                    </div>
                    <div className="text-[10px] text-neutral-500 truncate font-mono">{link.original_url}</div>
                  </div>
                ))}
             </div>
          </div>
        </div>

        {/* RIGHT PANEL */}
        <div className="flex-1 flex flex-col bg-[#050505]">
          <div className="flex-1 border-b border-[#222] flex flex-col min-h-[50%]">
             <div className="h-10 border-b border-[#222] bg-[#0a0a0a] flex items-center px-4">
                <Terminal className="w-3 h-3 text-neutral-500 mr-2" />
                <span className="text-xs font-mono text-neutral-500 uppercase">System Output</span>
             </div>
             <div className="flex-1 p-4 font-mono text-xs overflow-y-auto bg-[#030303] space-y-2">
                {logs.map((log, i) => (
                  <div key={i} className="flex gap-3 text-neutral-400 border-l-2 border-transparent hover:border-[#333] pl-2">
                    <span className="text-neutral-700 min-w-[70px]">{log.timestamp}</span>
                    <div className="flex-1 break-all">
                      {log.type === 'error' ? <span className="text-red-500 mr-2">ERR</span> : <span className="text-emerald-500 mr-2">OK</span>}
                      <span className="text-neutral-300">{log.message}</span>
                    </div>
                    {log.latency > 0 && <span className="text-emerald-600">{log.latency}ms</span>}
                  </div>
                ))}
                <div ref={logsEndRef} />
             </div>
          </div>

          <div className="h-[40%] bg-[#080808] flex flex-col">
             <div className="h-10 border-b border-[#222] flex items-center px-4">
                <Activity className="w-3 h-3 text-neutral-500 mr-2" />
                <span className="text-xs font-mono text-neutral-500 uppercase">Traffic</span>
             </div>
             <div className="flex-1 p-4">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={analytics}>
                    <XAxis dataKey="name" stroke="#333" fontSize={10} tickLine={false} axisLine={false} />
                    <Tooltip cursor={{fill: '#ffffff05'}} contentStyle={{ backgroundColor: '#111', border: '1px solid #333', fontSize: '11px', color: '#fff' }} />
                    <Bar dataKey="clicks" barSize={40}>
                      {analytics.map((entry, index) => <Cell key={`cell-${index}`} fill={['#3b82f6', '#10b981', '#6366f1'][index % 3]} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
             </div>
          </div>
        </div>
      </div>
    </div>
  );
}
