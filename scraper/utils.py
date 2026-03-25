from playwright.async_api import Page, Route


async def _bloquear_recursos(route: Route):
    """Función interna para interceptar y abortar recursos pesados."""
    # Tipos de archivo que no nos interesan para extraer texto
    recursos_excluidos = ["image", "media", "font"]

    if route.request.resource_type in recursos_excluidos:
        await route.abort()
    else:
        await route.continue_()


async def optimizar_pagina(page: Page):
    """
    Aplica el interceptor de red a una página de Playwright.
    Esto reduce drásticamente el consumo de CPU, RAM y ancho de banda.
    """
    await page.route("**/*", _bloquear_recursos)