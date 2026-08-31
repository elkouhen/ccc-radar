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

  return { computeLayerBands, rectanglesOverlap };
});
