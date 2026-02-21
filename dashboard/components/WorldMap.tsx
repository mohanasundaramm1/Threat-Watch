'use client';

import React, { memo } from 'react';
import {
    ComposableMap,
    Geographies,
    Geography,
    ZoomableGroup,
} from 'react-simple-maps';
import { scaleQuantize } from 'd3-scale';

// A lightweight TopoJSON map of the world, hosted locally
const geoUrl = "/data/features.json";

interface WorldMapProps {
    data: Record<string, number>;
}

const colorScale = scaleQuantize<string>()
    .domain([1, 100]) // Will be dynamically overriding this depending on data max
    .range([
        "#ffedea",
        "#ffcec5",
        "#ffad9f",
        "#ff8a75",
        "#ff5533",
        "#e2492d",
        "#be3d26",
        "#9a311f",
        "#782618"
    ]);

const WorldMap: React.FC<WorldMapProps> = ({ data }) => {
    // Find max value for dynamic scaling
    const maxValue = Math.max(...Object.values(data), 10);

    // Custom scale that stretches up to the highest threat count
    const dynamicScale = scaleQuantize<string>()
        .domain([1, maxValue])
        .range([
            "#2a0a18", // very dark red-purple (low threats)
            "#4a0a18",
            "#6a0b18",
            "#8a0c18",
            "#aa0d18",
            "#ca0e18",
            "#ea0f18",
            "#ff2a2a", // bright red (high threats)
        ]);

    return (
        <div className="w-full h-[400px] border border-slate-700/50 rounded-xl bg-slate-800/20 overflow-hidden relative">
            <ComposableMap
                projectionConfig={{
                    rotate: [-10, 0, 0],
                    scale: 147
                }}
                className="w-full h-full"
            >
                <ZoomableGroup center={[0, 20]}>
                    <Geographies geography={geoUrl}>
                        {({ geographies }) =>
                            geographies.map((geo) => {
                                const countryName = geo.properties.name;
                                // Try matching exact or common alternatives
                                const threatCount = data[countryName] ||
                                    (countryName === "United States of America" ? data["United States"] || 0 : 0) ||
                                    (countryName === "Russian Federation" ? data["Russia"] || 0 : 0);

                                const fillColor = threatCount > 0 ? dynamicScale(threatCount) : "#1e293b"; // base slate-800 color for safest countries

                                return (
                                    <Geography
                                        key={geo.rsmKey}
                                        geography={geo}
                                        fill={fillColor}
                                        stroke="#334155" // slate-700
                                        strokeWidth={0.5}
                                        style={{
                                            default: { outline: "none" },
                                            hover: {
                                                fill: threatCount > 0 ? "#22d3ee" : "#3b82f6", // cyan glow on hover
                                                outline: "none",
                                                cursor: "pointer",
                                                transition: "all 250ms"
                                            },
                                            pressed: { outline: "none" },
                                        }}
                                        onMouseEnter={() => {
                                            // Optional Tooltip logic could go here
                                        }}
                                    />
                                );
                            })
                        }
                    </Geographies>
                </ZoomableGroup>
            </ComposableMap>

            {/* Overlay legend placeholder */}
            <div className="absolute bottom-4 left-4 bg-slate-900/80 backdrop-blur-sm p-3 rounded-lg border border-slate-700 flex flex-col gap-1 text-xs">
                <div className="text-slate-300 font-bold mb-1">Threat Concentration</div>
                <div className="flex items-center gap-2">
                    <div className="w-4 h-4 rounded-sm bg-[#1e293b] border border-slate-700"></div>
                    <span className="text-slate-400">Zero Detected</span>
                </div>
                <div className="flex items-center gap-2">
                    <div className="w-4 h-4 rounded-sm bg-[#4a0a18]"></div>
                    <span className="text-slate-400">Low</span>
                </div>
                <div className="flex items-center gap-2">
                    <div className="w-4 h-4 rounded-sm bg-[#ff2a2a] shadow-[0_0_10px_rgba(255,42,42,0.5)]"></div>
                    <span className="text-slate-400">Critical</span>
                </div>
            </div>
        </div>
    );
};

export default memo(WorldMap);
