from playwright.async_api import Page, Route, Error


async def _bloquear_recursos(route: Route):
    """Función interna para interceptar y abortar recursos pesados con manejo de errores."""
    recursos_excluidos = ["image", "media", "font"]

    try:
        if route.request.resource_type in recursos_excluidos:
            # Intentamos abortar, pero si la página ya se cerró, lo ignoramos
            await route.abort()
        else:
            await route.continue_()
    except Error:
        # Si la página se cerró mientras decidíamos qué hacer, no pasa nada
        pass


async def optimizar_pagina(page: Page):
    """
    Aplica el interceptor de red a una página de Playwright.
    """
    # Agregamos un manejo preventivo para cuando la página se destruye
    await page.route("**/*", _bloquear_recursos)