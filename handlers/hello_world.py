"""Handler for hello world."""
from aiohttp import web


async def resource_get(request):
    """Returns {'Hello': 'World'}."""
    data = {'Hello': 'World'}

    return web.json_response(data=data, content_type='application/json')
