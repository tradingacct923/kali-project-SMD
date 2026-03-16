/**
 * Volume Bubble Renderer — LWC Custom Series Plugin
 *
 * Renders buy/sell volume bubbles from the `bp` (bubble profile) dict
 * produced by l2_worker.py's tick classification engine.
 *
 * LOD (Level of Detail) tiers:
 *   - barSpacing > 20px  → full bubbles with text (zoomed in)
 *   - barSpacing 6-20px  → small colored dots only (zoomed out)
 *   - barSpacing <= 5px  → nothing rendered (bird's eye)
 *
 * Performance optimizations:
 *   - Threshold filtering: only draws levels above MIN_BUBBLE_VOL
 *   - Canvas state batching: groups fills by color to minimize state changes
 *   - No fillText() at macro zoom
 *   - Uses media coordinate space (no manual DPR math)
 */

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════
const BUBBLE_CONFIG = {
    MIN_BUBBLE_VOL: 10,       // minimum total vol to draw a bubble
    MAX_RADIUS: 22,           // max bubble radius in px (prevents swallowing the chart)
    MIN_RADIUS: 3,            // min bubble radius
    DOT_RADIUS: 2.5,          // radius for macro-zoom dots
    FONT: '10px "JetBrains Mono", "SF Mono", monospace',
    BUY_COLOR: 'rgba(31, 209, 122, 0.55)',     // green (aggressive buy)
    SELL_COLOR: 'rgba(224, 48, 96, 0.55)',      // red (aggressive sell)
    BUY_COLOR_STRONG: 'rgba(31, 209, 122, 0.8)',
    SELL_COLOR_STRONG: 'rgba(224, 48, 96, 0.8)',
    TEXT_COLOR: 'rgba(255, 255, 255, 0.9)',
    NEUTRAL_COLOR: 'rgba(140, 160, 200, 0.3)',
};

// ═══════════════════════════════════════════════════════════════════════════
// RENDERER — draws bubbles on the LWC canvas
// ═══════════════════════════════════════════════════════════════════════════
class VolumeBubbleRenderer {
    constructor() {
        this._data = null;
    }

    update(data) {
        this._data = data;
    }

    draw(target, priceConverter) {
        const d = this._data;
        if (!d || !d.bars || d.bars.length === 0) return;

        const barSpacing = d.barSpacing || 6;

        // ── Bird's eye: skip rendering entirely ──
        if (barSpacing <= 5) return;

        const useDots = barSpacing <= 20;  // macro zoom: dots only
        const { from, to } = d.visibleRange;

        target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
            // ── Pre-compute volume scale ──
            // Find max volume across visible bars for radius normalization
            let maxVol = 0;
            for (let i = from; i < to; i++) {
                const bar = d.bars[i];
                if (!bar || !bar.originalData || !bar.originalData.bp) continue;
                const bp = bar.originalData.bp;
                for (const key in bp) {
                    const entry = bp[key];
                    const total = entry[0] + entry[1];
                    if (total > maxVol) maxVol = total;
                }
            }
            if (maxVol === 0) return;

            // ── Batch arrays for canvas state grouping ──
            // Separate buy vs sell draws to minimize fillStyle changes
            const buyBubbles = [];
            const sellBubbles = [];

            for (let i = from; i < to; i++) {
                const bar = d.bars[i];
                if (!bar || !bar.originalData || !bar.originalData.bp) continue;

                const bp = bar.originalData.bp;
                const x = bar.x;  // x coordinate from LWC

                for (const priceStr in bp) {
                    const entry = bp[priceStr];
                    const buyVol = entry[0];
                    const sellVol = entry[1];
                    const totalVol = buyVol + sellVol;

                    // Threshold filter: skip insignificant levels
                    if (totalVol < BUBBLE_CONFIG.MIN_BUBBLE_VOL) continue;

                    // Convert price to Y coordinate
                    const price = parseFloat(priceStr);
                    const y = priceConverter(price);
                    if (y === null || y === undefined) continue;

                    // Determine dominant side
                    const isBuy = buyVol >= sellVol;

                    // Calculate radius (normalized to maxVol, capped)
                    let radius;
                    if (useDots) {
                        radius = BUBBLE_CONFIG.DOT_RADIUS;
                    } else {
                        const ratio = totalVol / maxVol;
                        radius = BUBBLE_CONFIG.MIN_RADIUS +
                            ratio * (BUBBLE_CONFIG.MAX_RADIUS - BUBBLE_CONFIG.MIN_RADIUS);
                        radius = Math.min(radius, BUBBLE_CONFIG.MAX_RADIUS);
                    }

                    const bubble = { x, y, radius, totalVol, buyVol, sellVol };
                    if (isBuy) {
                        buyBubbles.push(bubble);
                    } else {
                        sellBubbles.push(bubble);
                    }
                }
            }

            // ── Draw buy bubbles (green) — batched ──
            if (buyBubbles.length > 0) {
                ctx.fillStyle = useDots
                    ? BUBBLE_CONFIG.BUY_COLOR_STRONG
                    : BUBBLE_CONFIG.BUY_COLOR;
                ctx.beginPath();
                for (const b of buyBubbles) {
                    ctx.moveTo(b.x + b.radius, b.y);
                    ctx.arc(b.x, b.y, b.radius, 0, Math.PI * 2);
                }
                ctx.fill();
            }

            // ── Draw sell bubbles (red) — batched ──
            if (sellBubbles.length > 0) {
                ctx.fillStyle = useDots
                    ? BUBBLE_CONFIG.SELL_COLOR_STRONG
                    : BUBBLE_CONFIG.SELL_COLOR;
                ctx.beginPath();
                for (const b of sellBubbles) {
                    ctx.moveTo(b.x + b.radius, b.y);
                    ctx.arc(b.x, b.y, b.radius, 0, Math.PI * 2);
                }
                ctx.fill();
            }

            // ── Draw text labels (zoomed in only) ──
            if (!useDots && (buyBubbles.length > 0 || sellBubbles.length > 0)) {
                ctx.fillStyle = BUBBLE_CONFIG.TEXT_COLOR;
                ctx.font = BUBBLE_CONFIG.FONT;
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';

                const allBubbles = buyBubbles.concat(sellBubbles);
                for (const b of allBubbles) {
                    // Only show text if radius is big enough to read
                    if (b.radius >= 8) {
                        const label = b.totalVol >= 1000
                            ? (b.totalVol / 1000).toFixed(1) + 'k'
                            : String(b.totalVol);
                        ctx.fillText(label, b.x, b.y);
                    }
                }
            }
        });
    }
}


// ═══════════════════════════════════════════════════════════════════════════
// CUSTOM SERIES VIEW — bridges LWC's plugin API to our renderer
// ═══════════════════════════════════════════════════════════════════════════
class VolumeBubbleSeries {
    constructor() {
        this._renderer = new VolumeBubbleRenderer();
    }

    /**
     * Return the renderer instance.
     */
    renderer() {
        return this._renderer;
    }

    /**
     * Called by LWC with the latest data + series options.
     * We forward bars/barSpacing/visibleRange to our renderer.
     */
    update(data, seriesOptions) {
        this._renderer.update(data);
    }

    /**
     * Interpret custom data for auto-scaling.
     * We return the high/low prices from bp keys so LWC
     * includes bubble positions in the visible price range.
     */
    priceValueBuilder(plotRow) {
        const bp = plotRow.bp;
        if (!bp || typeof bp !== 'object') {
            // No bubble data — use the close price for scaling
            return [plotRow.close || 0];
        }
        const prices = Object.keys(bp).map(Number).filter(p => !isNaN(p));
        if (prices.length === 0) return [plotRow.close || 0];
        return [Math.max(...prices), Math.min(...prices), plotRow.close || 0];
    }

    /**
     * Determine if a data point is whitespace (no data).
     */
    isWhitespace(data) {
        return !data || data.close === undefined;
    }

    /**
     * Default options for the series.
     */
    defaultOptions() {
        return {};
    }
}

// Export for use in app.js
// (since we're using plain <script> tags, we attach to window)
window.VolumeBubbleSeries = VolumeBubbleSeries;
window.BUBBLE_CONFIG = BUBBLE_CONFIG;
