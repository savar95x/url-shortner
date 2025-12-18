import React, { useState, useEffect, useRef } from 'react';
import { 
  ArrowRight, Activity, Terminal, Database, 
  Zap, Play, Layout, Server, Copy, Loader2 
} from 'lucide-react';
import { 
  BarChart, Bar, XAxis, Tooltip, ResponsiveContainer, Cell 
} from 'recharts';

// --- CONFIGURATION ---
const API_BASE = "https://url-shortner-backend-bhvv.onrender.com"; 
const ROUTER_BASE = "";

export default function App() {
  const [longUrl, setLongUrl] = useState('');
  const [generatedLinks, setGeneratedLinks] = useState([]);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [analytics, setAnalytics] = useState([]);
  const [isRedirecting, setIsRedirecting] = useState(false);
  const [isWakingUp, setIsWakingUp] = useState(true); // New state for initial connection
  const logsEndRef = useRef(null);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  // --- DEBUGGING ON MOUNT ---
  useEffect(() => {
    addLog(`Configured API: ${API_BASE}`, "system");
  }, []);

  const getRelativePath = () => {
    let path = window.location.pathname;
    if (ROUTER_BASE && path.startsWith(ROUTER_BASE)) {
      path = path.replace(ROUTER_BASE, '');
    }
    if (path.startsWith('/')) path = path.substring(1);
    if (path.endsWith('/')) path = path.substring(0, path.length - 1);
    return path;
  };

  useEffect(() => {
    const path = getRelativePath();
    if (path && path.trim() !== "") {
      setIsRedirecting(true);
      const performRedirect = async () => {
        try {
          addLog(`Attempting redirect for code: ${path}`, "system");
          const res = await fetch(`${API_BASE}/${path}`);
          if (!res.ok) throw new Error(`Backend Status: ${res.status}`);
          const data = await res.json();
          if (data.location) {
            window.location.href = data.location;
          } else {
            setIsRedirecting(false);
            window.history.pushState({}, "", ROUTER_BASE || "/"); 
            alert("Invalid Short URL");
          }
        } catch (err) {
          setIsRedirecting(false);
          console.error("Redirect failed", err);
          addLog(`Redirect Error: ${err.message}`, "error");
        }
      };
      performRedirect();
    }
  }, []);

  const addLog = (message, type = 'info', latency = 0) => {
    setLogs(prev => [...prev, { 
      message, type, latency, 
      timestamp: new Date().toLocaleTimeString().split(' ')[0] 
    }]);
  };

  const fetchData = async () => {
    if (getRelativePath().length > 0) return;
    try {
      const urlsRes = await fetch(`${API_BASE}/api/urls`);
      if (urlsRes.ok) {
        const data = await urlsRes.json();
        setGeneratedLinks(data);
        setIsWakingUp(false); // Backend is awake!
      }
      const statsRes = await fetch(`${API_BASE}/api/analytics`);
      if (statsRes.ok) {
        const data = await statsRes.json();
        setAnalytics(data);
      }
    } catch (err) {
      console.warn("Polling failed - Backend might be sleeping", err);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleShorten = async (e) => {
    e.preventDefault();
    if (!longUrl) return;
    setLoading(true);
    const start = performance.now();

    try {
      addLog(`Connecting to: ${API_BASE}/shorten`, "system");
      
      const res = await fetch(`${API_BASE}/shorten`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: longUrl })
      });

      if (!res.ok) throw new Error(`HTTP Error ${res.status}`);
      
      const data = await res.json();
      const latency = Math.round(performance.now() - start);
      
      const fullShortUrl = `${window.location.origin}${ROUTER_BASE}/${data.short_code}`;
      
      if (data.existed) {
        addLog(`Found Existing: ${fullShortUrl}`, "warning", latency);
      } else {
        addLog(`Created: ${fullShortUrl}`, "success", latency);
      }
      
      setLongUrl('');
      fetchData();

    } catch (error) {
      addLog(`Fail: ${error.message}`, "error");
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = (shortCode) => {
    const fullUrl = `${window.location.origin}${ROUTER_BASE}/${shortCode}`;
    navigator.clipboard.writeText(fullUrl).then(() => {
      addLog(`Copied: ${fullUrl}`, "success");
    }).catch(err => {
      addLog("Copy failed", "error");
    });
  };

  const handleSimulateVisit = async (shortCode) => {
    const start = performance.now();
    addLog(`GET /${shortCode}`, "system");
    try {
      const res = await fetch(`${API_BASE}/${shortCode}`);
      const data = await res.json();
      const latency = Math.round(performance.now() - start);
      if (data.location) {
        addLog(`Redirect -> ${data.location}`, "success", latency);
        window.open(data.location, '_blank');
        fetchData(); 
      } else {
         addLog(`Error: ${data.detail}`, "error");
      }
    } catch (err) {
      addLog(`Network Error: ${err.message}`, "error");
    }
  };

  if (isRedirecting) {
    return (
      <div className="h-screen bg-[#050505] flex items-center justify-center text-[#e5e5e5] font-mono">
        <div className="flex flex-col items-center gap-4">
          <div className="w-8 h-8 rounded-full border-2 border-emerald-500 border-t-transparent animate-spin"></div>
          <span className="animate-pulse text-sm tracking-widest uppercase">Redirecting...</span>
          <span className="text-xs text-neutral-600">Via: {API_BASE}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-[#050505] text-[#e5e5e5] overflow-hidden flex flex-col font-sans">
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;500;600&family=Playfair+Display:ital,wght@1,400&display=swap');
        .font-mono { font-family: 'JetBrains Mono', monospace; }
        .font-sans { font-family: 'Inter', sans-serif; }
        .font-serif { font-family: 'Playfair Display', serif; }
      `}</style>
      <header className="h-14 border-b border-[#222] flex items-center justify-between px-4 bg-[#0a0a0a] shrink-0">
        <div className="flex items-center gap-4">
          <a href="https://savar.is-a.dev" target="_blank" rel="noopener noreferrer" className="font-serif italic text-lg tracking-wide pr-4 border-r border-[#222] hover:text-white transition-colors">Savar</a>
          <div className="flex items-center gap-2 text-xs font-mono text-neutral-500">
            <Layout className="w-3 h-3" />
            <span className="hidden sm:inline">PRODUCTION</span>
            <span className="text-neutral-700 hidden sm:inline">/</span>
            <span className="text-white">DASHBOARD</span>
          </div>
        </div>
        <div className="flex items-center gap-2 px-3 py-1 rounded bg-[#111] border border-[#222]">
           <div className={`w-2 h-2 rounded-full ${!isWakingUp ? 'bg-emerald-500' : 'bg-yellow-500'} animate-pulse`}></div>
           <span className="text-[10px] font-mono text-neutral-400 uppercase">
             {isWakingUp ? "Connecting..." : "Live"}
           </span>
        </div>
      </header>
      <div className="flex-1 flex flex-col md:flex-row overflow-hidden">
        <div className="w-full md:w-[450px] border-b md:border-b-0 md:border-r border-[#222] flex flex-col bg-[#080808] h-1/2 md:h-full">
          <div className="p-6 border-b border-[#222]">
            <h2 className="text-xs font-mono text-neutral-500 uppercase tracking-widest mb-4 flex items-center gap-2"><Zap className="w-3 h-3" /> Generator</h2>
            <form onSubmit={handleShorten} className="space-y-4">
              <input type="url" required placeholder="https://..." className="w-full bg-[#0f0f0f] border border-[#222] rounded p-3 text-sm font-mono text-white focus:border-blue-900 focus:outline-none" value={longUrl} onChange={(e) => setLongUrl(e.target.value)} />
              <button disabled={loading || isWakingUp} className="w-full bg-white text-black hover:bg-neutral-200 disabled:opacity-50 text-xs font-bold uppercase tracking-widest py-3 rounded flex items-center justify-center gap-2">
                {isWakingUp ? "Server Waking Up..." : loading ? "Processing..." : "Shorten URL"} 
                {isWakingUp ? <Loader2 className="w-3 h-3 animate-spin" /> : <ArrowRight className="w-3 h-3" />}
              </button>
            </form>
          </div>
          <div className="flex-1 flex flex-col overflow-hidden">
             <div className="px-6 py-3 border-b border-[#222] bg-[#0a0a0a] flex justify-between items-center"><h2 className="text-xs font-mono text-neutral-500 uppercase flex items-center gap-2"><Database className="w-3 h-3" /> Records</h2></div>
             <div className="flex-1 overflow-y-auto p-2 space-y-1">
                {generatedLinks.length === 0 && <div className="text-center py-8 text-neutral-700 text-xs font-mono">{isWakingUp ? "Connecting to Backend..." : "No links yet"}</div>}
                {generatedLinks.map((link) => (
                  <div key={link.short_code} className="group p-3 rounded border border-transparent hover:border-[#333] hover:bg-[#111] cursor-default">
                    <div className="flex justify-between items-center mb-1">
                      <div className="flex items-center gap-2"><span className="text-blue-400 font-mono text-sm">/{link.short_code}</span><span className="text-[10px] text-neutral-600 font-mono">Clicks: {link.clicks}</span></div>
                      <div className="flex gap-2 opacity-100 md:opacity-0 group-hover:opacity-100 transition-opacity">
                        <button onClick={() => handleCopy(link.short_code)} className="bg-[#222] hover:bg-[#333] text-[10px] px-2 py-1 rounded text-white font-mono flex items-center gap-1">COPY <Copy className="w-2 h-2" /></button>
                        <button onClick={() => handleSimulateVisit(link.short_code)} className="bg-[#222] hover:bg-[#333] text-[10px] px-2 py-1 rounded text-white font-mono flex items-center gap-1">TEST <Play className="w-2 h-2" /></button>
                      </div>
                    </div>
                    <div className="text-[10px] text-neutral-500 truncate font-mono">{link.original_url}</div>
                  </div>
                ))}
             </div>
          </div>
        </div>
        <div className="flex-1 flex flex-col bg-[#050505] h-1/2 md:h-full">
          <div className="flex-1 border-b border-[#222] flex flex-col min-h-[50%]">
             <div className="h-10 border-b border-[#222] bg-[#0a0a0a] flex items-center px-4"><Terminal className="w-3 h-3 text-neutral-500 mr-2" /><span className="text-xs font-mono text-neutral-500 uppercase">System Output</span></div>
             <div className="flex-1 p-4 font-mono text-xs overflow-y-auto bg-[#030303] space-y-2">
                {logs.length === 0 && <div className="text-neutral-700">System ready.</div>}
                {logs.map((log, i) => (
                  <div key={i} className="flex gap-3 text-neutral-400 border-l-2 border-transparent hover:border-[#333] pl-2">
                    <span className="text-neutral-700 min-w-[70px]">{log.timestamp}</span>
                    <div className="flex-1 break-all">
                      {log.type === 'error' ? <span className="text-red-500 mr-2">ERR</span> : log.type === 'warning' ? <span className="text-yellow-500 mr-2">WARN</span> : <span className="text-emerald-500 mr-2">OK</span>}
                      <span className="text-neutral-300">{log.message}</span>
                    </div>
                    {log.latency > 0 && <span className="text-emerald-600">{log.latency}ms</span>}
                  </div>
                ))}
                <div ref={logsEndRef} />
             </div>
          </div>
          <div className="h-[40%] bg-[#080808] flex flex-col">
             <div className="h-10 border-b border-[#222] flex items-center px-4"><Activity className="w-3 h-3 text-neutral-500 mr-2" /><span className="text-xs font-mono text-neutral-500 uppercase">Traffic</span></div>
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
