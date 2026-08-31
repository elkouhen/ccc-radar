(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.SystemLensLayerGeometry = factory();
})(typeof globalThis === "object" ? globalThis : this, function () {
  function computeLayerBands(layerBounds, options) {
    const {
      contentMinX,
      contentMaxX,
      viewportWidth,
      titleGutter = 182,
      minimumWidth = 260,
    } = options;
    const left = Math.max(0, contentMinX - titleGutter);
    const right = Math.min(viewportWidth, contentMaxX);
    const width = Math.max(minimumWidth, right - left);
    return layerBounds.map((bounds, index) => {
      const previous = layerBounds[index - 1];
      const next = layerBounds[index + 1];
      const top = previous
        ? (previous.contentBottom + bounds.contentTop) / 2
        : bounds.contentTop;
      const bottom = next
        ? (bounds.contentBottom + next.contentTop) / 2
        : bounds.contentBottom;
      return { left, top: Math.max(0, top), width, height: Math.max(0, bottom - Math.max(0, top)) };
    });
  }

  function rectanglesOverlap(left, right) {
    return left.left < right.left + right.width
      && left.left + left.width > right.left
      && left.top < right.top + right.height
      && left.top + left.height > right.top;
  }

  function computeClusterSubLayers(microservices, resources, options) {
    const { nodeGapX = 320, nodeGapY = 220, subLayerGapY = 160, maxColumns = 5 } = options;
    const positions = {};
    const groups = [microservices, resources].filter(group => group.length);
    // Sigma's graph Y axis grows upwards.  Keep services at the upper
    // sub-layer (higher graph Y) and move resources towards negative Y.
    let cursorY = 0;
    let width = 0;
    groups.forEach((group, groupIndex) => {
      const columns = Math.min(maxColumns, Math.max(1, Math.ceil(Math.sqrt(group.length))));
      const rows = Math.ceil(group.length / columns);
      width = Math.max(width, (columns - 1) * nodeGapX);
      group.forEach((id, index) => {
        positions[id] = {
          x: (index % columns) * nodeGapX,
          y: cursorY - Math.floor(index / columns) * nodeGapY,
          group: groupIndex,
        };
      });
      cursorY -= (rows - 1) * nodeGapY;
      if (groupIndex === 0 && groups.length > 1) cursorY -= subLayerGapY;
    });
    return { positions, width, height: cursorY };
  }

  return { computeLayerBands, computeClusterSubLayers, rectanglesOverlap };
});
