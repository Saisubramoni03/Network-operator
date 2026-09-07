import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import NetworkOverview from "./pages/NetworkOverview";

function App() {
  return (
    <BrowserRouter>
      <nav style={{ padding: "1rem", borderBottom: "1px solid #ddd" }}>
        <NavLink to="/" style={{ marginRight: "1rem" }}>Overview</NavLink>
      </nav>
      <div style={{ padding: "1rem" }}>
        <Routes>
          <Route path="/" element={<NetworkOverview />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;