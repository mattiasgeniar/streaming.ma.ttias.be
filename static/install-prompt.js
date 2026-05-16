// Vanilla port of https://github.com/mattiasgeniar/pwa-install-prompt
// - Captures `beforeinstallprompt` so we can trigger the install flow.
// - On iOS (no native prompt), shows manual "Add to Home Screen" instructions.
// - Respects a localStorage dismissal so we don't nag.
(function () {
  const DISMISS_KEY = 'pwa-install-dismissed-at';
  const DISMISS_TTL_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

  const detectStandalone = () =>
    (typeof window !== 'undefined' &&
      window.matchMedia &&
      window.matchMedia('(display-mode: standalone)').matches) ||
    (typeof navigator !== 'undefined' && navigator.standalone === true);

  const detectIOS = () => {
    const ua = navigator.userAgent || '';
    const platform = navigator.platform || '';
    if (/iPad|iPhone|iPod/.test(ua)) return true;
    // iPadOS 13+ reports as MacIntel with touch support
    return platform === 'MacIntel' && navigator.maxTouchPoints > 1;
  };

  const detectMobile = () => {
    const ua = navigator.userAgent || '';
    return detectIOS() || /Android|Mobile/i.test(ua);
  };

  const wasRecentlyDismissed = () => {
    try {
      const raw = localStorage.getItem(DISMISS_KEY);
      if (!raw) return false;
      return Date.now() - Number(raw) < DISMISS_TTL_MS;
    } catch (e) {
      return false;
    }
  };

  const markDismissed = () => {
    try {
      localStorage.setItem(DISMISS_KEY, String(Date.now()));
    } catch (e) {}
  };

  // Register service worker — required for the beforeinstallprompt event.
  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker
        .register('/service-worker.js', { scope: '/' })
        .catch(() => {});
    });
  }

  const isIOS = detectIOS();
  const mobile = detectMobile();
  let promptEvent = null;
  let installed = detectStandalone();
  let bannerEl = null;

  const buildBanner = () => {
    const el = document.createElement('div');
    el.className = 'pwa-install';
    el.setAttribute('role', 'status');
    el.setAttribute('aria-live', 'polite');

    const icon = document.createElement('div');
    icon.className = 'pwa-install__icon';
    icon.textContent = '\uD83D\uDCF2'; // 📲

    const body = document.createElement('div');
    body.className = 'pwa-install__body';
    const title = document.createElement('div');
    title.className = 'pwa-install__title';
    const subtitle = document.createElement('div');
    subtitle.className = 'pwa-install__subtitle';

    if (isIOS) {
      title.textContent = 'Add as an app';
      subtitle.textContent = "Tap Share, then 'Add to Home Screen'.";
    } else {
      title.textContent = 'Add to your home screen';
      subtitle.textContent = 'Opens like an app, no browser chrome.';
    }
    body.appendChild(title);
    body.appendChild(subtitle);

    const actions = document.createElement('div');
    actions.className = 'pwa-install__actions';

    if (!isIOS) {
      const cta = document.createElement('button');
      cta.type = 'button';
      cta.className = 'pwa-install__cta';
      cta.textContent = 'Install';
      cta.addEventListener('click', async () => {
        if (!promptEvent) return;
        const ev = promptEvent;
        promptEvent = null;
        try {
          await ev.prompt();
          const choice = await ev.userChoice;
          if (choice && choice.outcome === 'dismissed') markDismissed();
        } catch (e) {}
        hideBanner();
      });
      actions.appendChild(cta);
    }

    const dismiss = document.createElement('button');
    dismiss.type = 'button';
    dismiss.className = 'pwa-install__dismiss';
    dismiss.setAttribute('aria-label', 'Dismiss');
    dismiss.textContent = '\u00D7';
    dismiss.addEventListener('click', () => {
      markDismissed();
      hideBanner();
    });
    actions.appendChild(dismiss);

    el.appendChild(icon);
    el.appendChild(body);
    el.appendChild(actions);
    return el;
  };

  const showBanner = () => {
    if (bannerEl || installed || wasRecentlyDismissed()) return;
    if (!mobile) return;
    if (!isIOS && !promptEvent) return;
    bannerEl = buildBanner();
    document.body.appendChild(bannerEl);
    requestAnimationFrame(() => bannerEl.classList.add('pwa-install--visible'));
  };

  const hideBanner = () => {
    if (!bannerEl) return;
    const el = bannerEl;
    bannerEl = null;
    el.classList.remove('pwa-install--visible');
    setTimeout(() => el.remove(), 250);
  };

  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    promptEvent = e;
    showBanner();
  });

  window.addEventListener('appinstalled', () => {
    installed = true;
    promptEvent = null;
    markDismissed();
    hideBanner();
  });

  if (window.matchMedia) {
    window.matchMedia('(display-mode: standalone)').addEventListener?.('change', (m) => {
      installed = m.matches;
      if (installed) hideBanner();
    });
  }

  // iOS never fires beforeinstallprompt — show the manual instructions banner.
  if (isIOS && !installed) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', showBanner);
    } else {
      showBanner();
    }
  }
})();
