(() => {
  const menuButton = document.querySelector("[data-menu-button]");
  const navigation = document.querySelector("[data-navigation]");

  if (menuButton && navigation) {
    menuButton.addEventListener("click", () => {
      const expanded = menuButton.getAttribute("aria-expanded") === "true";
      menuButton.setAttribute("aria-expanded", String(!expanded));
      navigation.dataset.open = String(!expanded);
    });

    navigation.addEventListener("click", (event) => {
      if (event.target.closest("a")) {
        menuButton.setAttribute("aria-expanded", "false");
        navigation.dataset.open = "false";
      }
    });
  }

  document.querySelectorAll("[data-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const target = document.getElementById(button.dataset.copy);
      if (!target) return;
      const original = button.textContent;
      try {
        await navigator.clipboard.writeText(target.textContent.trim());
        button.textContent = "Copied";
      } catch {
        button.textContent = "Select to copy";
        const selection = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(target);
        selection.removeAllRanges();
        selection.addRange(range);
      }
      window.setTimeout(() => {
        button.textContent = original;
      }, 1600);
    });
  });

  const articleLinks = [...document.querySelectorAll("[data-article-link]")];
  const sections = articleLinks
    .map((link) => document.querySelector(link.getAttribute("href")))
    .filter(Boolean);

  if (articleLinks.length && sections.length && "IntersectionObserver" in window) {
    const observer = new IntersectionObserver((entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
      if (!visible) return;
      articleLinks.forEach((link) => {
        const active = link.getAttribute("href") === `#${visible.target.id}`;
        link.toggleAttribute("aria-current", active);
      });
    }, { rootMargin: "-18% 0px -68%", threshold: [0, 0.2, 0.6] });
    sections.forEach((section) => observer.observe(section));
  }
})();
