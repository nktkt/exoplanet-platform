"""Small reusable UI + plotting helpers for the Streamlit dashboard.

These helpers are intentionally side-effect-light (except for the ones that
render directly into Streamlit) and free of heavyweight imports at module
level so that the dashboard module is cheap to import from the launcher.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import plotly.graph_objects as go

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exoplanet_platform.domain import LightCurve, Planet, Star


def planet_card(planet: Planet) -> None:
    """Render a compact :class:`streamlit.container` summarising a planet."""
    import streamlit as st

    with st.container(border=True):
        st.subheader(planet.identifier)
        st.caption(
            f"Host star **{planet.host_star}** · "
            f"{planet.discovery_method.value if planet.discovery_method else 'unknown'} · "
            f"{planet.catalog.value if planet.catalog else 'n/a'}"
        )
        cols = st.columns(4)
        cols[0].metric(
            "Period (d)",
            f"{planet.orbital_period_days:.4f}" if planet.orbital_period_days else "—",
        )
        cols[1].metric(
            "Radius (R⊕)",
            f"{planet.radius_earth:.2f}" if planet.radius_earth else "—",
        )
        cols[2].metric(
            "Mass (M⊕)",
            f"{planet.mass_earth:.2f}" if planet.mass_earth else "—",
        )
        cols[3].metric(
            "T_eq (K)",
            f"{planet.equilibrium_temperature_k:.0f}"
            if planet.equilibrium_temperature_k
            else "—",
        )


def hz_diagram(
    star: Star,
    planets: list[Planet],
    hz_bounds: tuple[float, float, float, float],
) -> go.Figure:
    """Return a top-down habitable-zone diagram.

    Parameters
    ----------
    star:
        Host star; rendered as a filled marker at the origin.
    planets:
        Planets orbiting ``star``. Any planet with a known semi-major axis is
        drawn as a circle at that radius.
    hz_bounds:
        ``(optimistic_inner, conservative_inner, conservative_outer, optimistic_outer)``
        in AU.
    """
    opt_in, cons_in, cons_out, opt_out = hz_bounds
    fig = go.Figure()
    theta = np.linspace(0.0, 2.0 * np.pi, 361)

    def _ring(radius: float, color: str, name: str, width: float = 2.0) -> None:
        fig.add_trace(
            go.Scatter(
                x=radius * np.cos(theta),
                y=radius * np.sin(theta),
                mode="lines",
                line={"color": color, "width": width},
                name=name,
                hoverinfo="name",
            )
        )

    # Fill optimistic HZ shell.
    fig.add_shape(
        type="circle",
        x0=-opt_out,
        y0=-opt_out,
        x1=opt_out,
        y1=opt_out,
        line_color="rgba(0, 180, 90, 0.2)",
        fillcolor="rgba(0, 180, 90, 0.12)",
        layer="below",
    )
    fig.add_shape(
        type="circle",
        x0=-opt_in,
        y0=-opt_in,
        x1=opt_in,
        y1=opt_in,
        line_color="rgba(0, 180, 90, 0.0)",
        fillcolor="rgba(255, 255, 255, 1.0)",
        layer="below",
    )

    _ring(opt_in, "#5fa3ff", "Optimistic inner")
    _ring(cons_in, "#00b45a", "Conservative inner")
    _ring(cons_out, "#00b45a", "Conservative outer")
    _ring(opt_out, "#5fa3ff", "Optimistic outer")

    fig.add_trace(
        go.Scatter(
            x=[0.0],
            y=[0.0],
            mode="markers",
            marker={"size": 18, "color": "gold", "line": {"color": "orange", "width": 2}},
            name=star.identifier,
            hovertext=[
                f"{star.identifier}"
                + (
                    f"<br>T_eff={star.effective_temperature_k:.0f} K"
                    if star.effective_temperature_k
                    else ""
                )
            ],
        )
    )

    xs, ys, labels = [], [], []
    for p in planets:
        if p.semi_major_axis_au is None:
            continue
        xs.append(p.semi_major_axis_au)
        ys.append(0.0)
        labels.append(p.identifier)
    if xs:
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers+text",
                marker={"size": 10, "color": "#333"},
                text=labels,
                textposition="top center",
                name="Planets",
            )
        )

    span = max(opt_out * 1.2, (max(xs) if xs else 0.0) * 1.2, 0.5)
    fig.update_layout(
        title=f"Habitable zone around {star.identifier}",
        xaxis={
            "title": "AU",
            "range": [-span, span],
            "zeroline": False,
            "scaleanchor": "y",
            "scaleratio": 1.0,
        },
        yaxis={"title": "AU", "range": [-span, span], "zeroline": False},
        showlegend=True,
        height=600,
    )
    return fig


def light_curve_plot(lc: LightCurve) -> go.Figure:
    """Return a plotly scatter of time vs flux for a :class:`LightCurve`."""
    fig = go.Figure()
    fig.add_trace(
        go.Scattergl(
            x=list(lc.time_bjd),
            y=list(lc.flux),
            mode="markers",
            marker={"size": 3, "color": "#1f77b4", "opacity": 0.6},
            name=lc.target,
        )
    )
    fig.update_layout(
        title=f"{lc.target} — {lc.mission.value}"
        + (f" (sector {lc.sector})" if lc.sector else "")
        + (f" (quarter {lc.quarter})" if lc.quarter else ""),
        xaxis_title="Time (BJD)",
        yaxis_title="Flux",
        height=450,
    )
    return fig


def periodogram_plot(period_days: np.ndarray, power: np.ndarray) -> go.Figure:
    """Return a plotly line plot of BLS power versus period."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=np.asarray(period_days),
            y=np.asarray(power),
            mode="lines",
            line={"color": "#d62728", "width": 1},
            name="BLS power",
        )
    )
    fig.update_layout(
        title="BLS periodogram",
        xaxis_title="Period (days)",
        yaxis_title="Power",
        xaxis_type="log",
        height=400,
    )
    return fig
