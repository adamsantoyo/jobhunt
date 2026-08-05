import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  // Overrides for contexts smaller than a full routed page (e.g. the
  // run-panel strip in AppShell), where the default copy reads oddly.
  title?: string;
  hint?: string;
}
interface State {
  error: Error | null;
}

// Crash barrier. AppShell mounts one keyed by pathname+search around the
// routed Outlet, and a second, unkeyed one around the run-panel/sweep-
// progress strip, so one component's render throw cannot blank the whole SPA.
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("ErrorBoundary caught an error:", error, info.componentStack);
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;
    const title = this.props.title ?? "This page hit an error.";
    const hint =
      this.props.hint ?? "The rest of the app still works. Use the nav on the left to keep going.";
    return (
      <div className="page-error settings-section">
        <h2>{title}</h2>
        <p className="confirm-text">{hint}</p>
        {error.message && <p className="muted-sm">{error.message}</p>}
        <button type="button" className="btn btn-primary" onClick={() => window.location.reload()}>
          Reload
        </button>
      </div>
    );
  }
}
