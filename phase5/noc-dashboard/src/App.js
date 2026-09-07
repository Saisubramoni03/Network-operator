import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import NetworkOverview from "./pages/NetworkOverview";
import GridExplorer from "./pages/GridExplorer";
import HotspotsAlerts from "./pages/HotspotsAlerts";
import PredictiveRisk from "./pages/PredictiveRisk";
import { MilanGridProvider } from "./context/MilanGridContext";
import "./App.css";

function App() {
  return (
    <MilanGridProvider>
      <BrowserRouter>
        <div className="app-shell">
          <nav className="app-nav">
            <div className="app-nav-brand">NOC Dashboard</div>
            <div className="app-nav-links">
              <NavLink to="/" end className="app-nav-link">Overview</NavLink>
              <NavLink to="/grid" className="app-nav-link">Grid Explorer</NavLink>
              <NavLink to="/hotspots" className="app-nav-link">Hotspots & Alerts</NavLink>
              <NavLink to="/predict" className="app-nav-link">Predictive Risk</NavLink>
            </div>
          </nav>
          <main className="app-content">
            <Routes>
              <Route path="/" element={<NetworkOverview />} />
              <Route path="/grid" element={<GridExplorer />} />
              <Route path="/hotspots" element={<HotspotsAlerts />} />
              <Route path="/predict" element={<PredictiveRisk />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </MilanGridProvider>
  );
}

export default App;