// fs-focus-nav.js
// Shared helper for the logo triple-click "focus" mode.

(function (global) {
  function installLogoFocusNav(opts) {
    const cfg = Object.assign({
      logoSelector: 'header .logo',
      headerSelector: 'header .row',
      tripleClickWindowMs: 600,
      redirectToIndex: true
    }, opts || {});

    const logo = document.querySelector(cfg.logoSelector);
    const header = document.querySelector(cfg.headerSelector);
    if (!logo || !header) return;

    let clickCount = 0;
    let clickTimer = null;
    let focusMode = false;

    logo.addEventListener('click', () => {
      clickCount++;
      clearTimeout(clickTimer);
      clickTimer = setTimeout(() => { clickCount = 0; }, cfg.tripleClickWindowMs);

      if (clickCount === 3) {
        clickCount = 0;
        focusMode = !focusMode;

        if (focusMode) {
          header.querySelectorAll('a, span.muted').forEach(el => {
            if (!el.closest('.brand')) el.style.display = 'none';
          });

          if (cfg.redirectToIndex &&
              !location.pathname.endsWith('index.html')) {
            location.href = 'index.html';
          }
        } else {
          header.querySelectorAll('a, span.muted').forEach(el => {
            el.style.display = '';
          });
        }
      }
    });
  }

  global.fsFocusNav = { installLogoFocusNav };

  // Optional legacy global for inline usage
  if (!global.installLogoFocusNav) {
    global.installLogoFocusNav = installLogoFocusNav;
  }
})(window);
