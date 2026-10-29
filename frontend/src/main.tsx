import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles/foundation.css";
import "./styles/shell.css";
import "./styles/overview.css";
import "./styles/workbench.css";
import "./styles/tools.css";
import "./styles/ui-system.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
