"""Handler for kudos."""
import datetime
import json
import os
import psycopg2

from aiohttp import web
import main


async def collection_get(request):
    """Returns kudos and 200."""
    app = await main.get_app()
    async with app.app['db_conn'].acquire() as conn:
        async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            sql = "SELECT * FROM kudos"
            await db.execute(sql)
            rows = await db.fetchall()

    data = []
    for row in rows:
        datum = {
            'id': row['id'],
            'kudo': row['kudo'],
            'updated_dt': main.serialize(row['updated_dt']),
            'created_dt': main.serialize(row['created_dt'])
        }

        data.append(datum)

    return web.json_response(data=data, content_type='application/json')

async def collection_post(request):
    """Returns posted kudos and 201."""
    new_kudo = json.loads(request._read_bytes)

    app = await main.get_app()
    async with app.app['db_conn'].acquire() as conn:
        async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            sql = "INSERT INTO kudos (kudo) VALUES (%(new_kudo)s) RETURNING *"
            await db.execute(sql, {'new_kudo': new_kudo['kudo']})
            row = await db.fetchone()

    data = {
        'id': row['id'],
        'kudo': row['kudo'],
        'updated_dt': main.serialize(row['updated_dt']),
        'created_dt': main.serialize(row['created_dt'])
    }

    return web.json_response(data=data, status=201, content_type='application/json')

async def resource_get(request, kudos_id):
    """Returns kudos with kudos_id and 200."""
    app = await main.get_app()
    async with app.app['db_conn'].acquire() as conn:
        async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            sql = "SELECT * FROM kudos WHERE id=%(kudos_id)s"
            await db.execute(sql, {'kudos_id': kudos_id})
            row = await db.fetchone()

    data = {
        'id': row['id'],
        'kudo': row['kudo'],
        'updated_dt': main.serialize(row['updated_dt']),
        'created_dt': main.serialize(row['created_dt'])
    }

    return web.json_response(data=data, content_type='application/json')

async def resource_put(request, kudos_id):
    """Updates and returns kudos with kudos_id and 200."""
    update_kudo = json.loads(request._read_bytes)

    # there's an interesting happenstance where SOMETIMES updated_dt < created_dt
    # hack by definitely adding 1s to updated_dt
    updated_dt = main.serialize(datetime.datetime.utcnow() + datetime.timedelta(seconds=1))

    app = await main.get_app()
    async with app.app['db_conn'].acquire() as conn:
        async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            sql = "UPDATE kudos SET kudo=%(update_kudo)s, updated_dt=%(updated_dt)s WHERE id=%(kudos_id)s RETURNING *"
            await db.execute(sql, {'update_kudo': update_kudo['kudo'], 'updated_dt': updated_dt, 'kudos_id': kudos_id})
            row = await db.fetchone()

    data = {
        'id': row['id'],
        'kudo': row['kudo'],
        'updated_dt': main.serialize(row['updated_dt']),
        'created_dt': main.serialize(row['created_dt'])
    }

    return web.json_response(data=data, content_type='application/json')

async def resource_delete(request, kudos_id):
    """Deletes kudos with kudos_id and returns a 204."""
    app = await main.get_app()
    async with app.app['db_conn'].acquire() as conn:
        async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as db:
            sql = "DELETE FROM kudos WHERE id=%(kudos_id)s"
            await db.execute(sql, {'kudos_id': kudos_id})

    return web.json_response(data={}, status=204, content_type='application/json')
