import { useEffect, useRef, type CSSProperties, type ReactNode } from "react";

// Minimal absolutely-positioned popover: closes on outside-click or Esc.
// Anchor it inside a `position: relative` wrapper around the trigger button.

const menuStyle: CSSProperties = {
  position: "absolute",
  top: "calc(100% + 4px)",
  right: 0,
  zIndex: 20,
  background: "var(--bg-2)",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius-sm)",
  boxShadow: "0 6px 16px rgba(0, 0, 0, 0.35)",
  padding: 4,
  display: "flex",
  flexDirection: "column",
  gap: 2,
  minWidth: 130,
};

const itemStyle: CSSProperties = {
  justifyContent: "flex-start",
  border: "none",
  background: "transparent",
  width: "100%",
};

export function Menu({ onClose, children }: { onClose: () => void; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const onDocMouseDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("mousedown", onDocMouseDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onDocMouseDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [onClose]);

  return (
    <div ref={ref} style={menuStyle} role="menu">
      {children}
    </div>
  );
}

export function MenuItem({ onClick, children }: { onClick: () => void; children: ReactNode }) {
  return (
    <button type="button" className="btn btn-sm" style={itemStyle} role="menuitem" onClick={onClick}>
      {children}
    </button>
  );
}
