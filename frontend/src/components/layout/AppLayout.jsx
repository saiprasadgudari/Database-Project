export default function AppLayout({ children }) {
  return (
    <div className="app-shell">
      <div className="app-shell-inner">
        <header className="app-header">
          <div>
            <h1 className="app-title">NYC Yellow Taxi Analytics</h1>
            <p className="app-subtitle">
              Explore urban mobility patterns from TLC Yellow Taxi trips.
            </p>
          </div>

          <div className="row">
            <span className="app-badge">PostgreSQL · Flask · React</span>
          </div>
        </header>

        <main className="app-main">{children}</main>
      </div>
    </div>
  );
}
