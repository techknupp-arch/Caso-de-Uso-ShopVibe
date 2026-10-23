"""Dimensional sales model. Order value is not payment revenue."""
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from dateutil import tz

META = ("source_system", "source_event_id", "source_object", "event_at", "is_simulated", "quality_flags")
KEYS = {"dim_cliente": ("cliente_id",), "dim_produto": ("produto_id",), "dim_data": ("data_id",),
        "fato_pedido": ("pedido_id",), "fato_item_pedido": ("pedido_id", "numero_linha"),
        "vendas_diarias": ("data_id", "status", "moeda", "is_simulated"),
        "estoque_atual": ("produto_id", "deposito_id")}


def _meta(row):
    return {field: row.get(field, [] if field == "quality_flags" else None) for field in META}


def _index(rows, keys):
    result = {}
    for row in rows:
        key = tuple(row[field] for field in keys)
        if None in key or key in result:
            raise ValueError("Chave Gold nula ou duplicada: " + str(key))
        result[key] = row
    return result


def _daily(orders):
    groups = {}
    for row in orders:
        key = (row["data_id"], row["status"], row["moeda"], row["is_simulated"])
        group = groups.setdefault(key, dict(zip(KEYS["vendas_diarias"], key), quantidade_pedidos=0,
                                           quantidade_unidades=0, valor_pedidos=Decimal("0.00")))
        group["quantidade_pedidos"] += 1
        group["quantidade_unidades"] += row["quantidade_unidades"]
        group["valor_pedidos"] += row["valor_pedido"]
    for row in groups.values():
        row["ticket_medio"] = (row["valor_pedidos"] / row["quantidade_pedidos"]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return [groups[key] for key in sorted(groups)]


def materialize(source, business_timezone="America/Sao_Paulo"):
    zone = tz.gettz(business_timezone)
    if zone is None:
        raise ValueError("Fuso Gold desconhecido: " + business_timezone)
    dates = set()

    def date_key(stamp):
        if not isinstance(stamp, datetime) or stamp.tzinfo is None:
            raise ValueError("Data Gold deve possuir timezone")
        local = stamp.astimezone(zone).date()
        dates.add(local)
        return int(local.strftime("%Y%m%d"))

    customers = {row["customer_id"]: dict(cliente_id=row["customer_id"], nome=row["name"], email=row["email"],
                                        is_deleted=row["is_deleted"], is_unknown=False, **_meta(row))
                 for row in source["customers"]}
    products = {row["product_id"]: dict(produto_id=row["product_id"], nome=row["name"], preco_atual=row["price"],
                                      is_deleted=row["is_deleted"], is_unknown=False, **_meta(row))
                for row in source["products"]}

    def ensure_dimension(mapping, identifier, field):
        if not identifier:
            raise ValueError("Identificador Gold obrigatorio ausente: " + field)
        if identifier not in mapping:
            row = dict(_meta({}), **{field: identifier}, nome="Cadastro nao encontrado",
                       is_deleted=False, is_unknown=True)
            row["email" if field == "cliente_id" else "preco_atual"] = None
            mapping[identifier] = row

    active_orders = {row["order_id"]: row for row in source["orders"] if not row["is_deleted"]}
    deleted_orders = {row["order_id"] for row in source["orders"] if row["is_deleted"]}
    items, grouped_items = [], defaultdict(list)
    for row in source["order_items"]:
        if row["is_deleted"] or row["order_id"] in deleted_orders:
            continue
        if row["order_id"] not in active_orders:
            raise ValueError("Item Gold sem pedido: " + row["order_id"])
        order = active_orders[row["order_id"]]
        ensure_dimension(products, row["product_id"], "produto_id")
        ensure_dimension(customers, order["customer_id"], "cliente_id")
        item = dict(pedido_id=row["order_id"], numero_linha=row["line_number"], produto_id=row["product_id"],
                    cliente_id=order["customer_id"], data_id=date_key(order["ordered_at"]), status=order["status"],
                    moeda=order["currency"], quantidade=row["quantity"], valor_unitario=row["unit_price"],
                    valor_item=row["line_amount"], **_meta(row))
        items.append(item)
        grouped_items[row["order_id"]].append(item)
    orders = []
    for row in active_orders.values():
        ensure_dimension(customers, row["customer_id"], "cliente_id")
        detail = grouped_items[row["order_id"]]
        fact = dict(pedido_id=row["order_id"], cliente_id=row["customer_id"], data_id=date_key(row["ordered_at"]),
                    pedido_em=row["ordered_at"], status=row["status"], moeda=row["currency"],
                    metodo_pagamento=row["payment_method"], valor_pedido=row["total_amount"],
                    quantidade_linhas=len(detail), quantidade_unidades=sum(item["quantidade"] for item in detail), **_meta(row))
        if not detail:
            fact["quality_flags"] = sorted(set(fact["quality_flags"]) | {"order_without_items"})
        orders.append(fact)
    stock = []
    for row in source["inventory"]:
        if row["is_deleted"]:
            continue
        ensure_dimension(products, row["product_id"], "produto_id")
        stock.append(dict(produto_id=row["product_id"], deposito_id=row["warehouse_id"],
                          data_id=date_key(row["stock_at"]), saldo_fisico=row["physical_stock"],
                          saldo_virtual=row["virtual_stock"], observado_em=row["stock_at"], **_meta(row)))
    calendar = []
    if dates:
        day, last = min(dates), max(dates)
        while day <= last:
            calendar.append(dict(data_id=int(day.strftime("%Y%m%d")), data=day, ano=day.year, mes=day.month,
                                 dia=day.day, trimestre=(day.month - 1) // 3 + 1,
                                 dia_semana=day.isoweekday(), fim_semana=day.isoweekday() >= 6))
            day += timedelta(days=1)
    tables = dict(dim_cliente=list(customers.values()), dim_produto=list(products.values()), dim_data=calendar,
                  fato_pedido=orders, fato_item_pedido=items, vendas_diarias=_daily(orders), estoque_atual=stock)
    for name in tables:
        tables[name].sort(key=lambda row: tuple(row[field] for field in KEYS[name]))
    validate(tables, business_timezone)
    return tables


def validate(tables, business_timezone):
    if set(tables) != set(KEYS):
        raise ValueError("Tabelas Gold incompletas")
    index = {name: _index(rows, KEYS[name]) for name, rows in tables.items()}
    zone = tz.gettz(business_timezone)
    if zone is None:
        raise ValueError("Fuso Gold desconhecido")
    for row in tables["dim_data"]:
        if row["data_id"] != int(row["data"].strftime("%Y%m%d")):
            raise ValueError("Chave da dimensao data divergente")
    detail = defaultdict(list)
    for row in tables["fato_item_pedido"]:
        parent = index["fato_pedido"].get((row["pedido_id"],))
        if parent is None or any(row[key] != parent[key] for key in ("cliente_id", "data_id", "status", "moeda", "is_simulated")):
            raise ValueError("Item Gold divergente do pedido")
        if (row["produto_id"],) not in index["dim_produto"]:
            raise ValueError("Produto Gold ausente")
        if row["quantidade"] <= 0 or row["valor_unitario"] < 0 or row["valor_item"] != row["quantidade"] * row["valor_unitario"]:
            raise ValueError("Valor de item Gold divergente")
        detail[row["pedido_id"]].append(row)
    for row in tables["fato_pedido"]:
        if (row["cliente_id"],) not in index["dim_cliente"] or (row["data_id"],) not in index["dim_data"]:
            raise ValueError("Dimensao Gold ausente no pedido")
        if row["data_id"] != int(row["pedido_em"].astimezone(zone).strftime("%Y%m%d")):
            raise ValueError("Dia do pedido Gold divergente do fuso")
        if row["valor_pedido"] < 0 or not row["status"] or not row["moeda"] or not isinstance(row["is_simulated"], bool):
            raise ValueError("Pedido Gold invalido")
        items = detail[row["pedido_id"]]
        if row["quantidade_linhas"] != len(items) or row["quantidade_unidades"] != sum(item["quantidade"] for item in items):
            raise ValueError("Quantidade de itens Gold divergente")
        if items and row["valor_pedido"] != sum(item["valor_item"] for item in items):
            raise ValueError("Total do pedido diverge dos itens: " + row["pedido_id"])
    if sorted(tables["vendas_diarias"], key=lambda row: tuple(row[key] for key in KEYS["vendas_diarias"])) != _daily(tables["fato_pedido"]):
        raise ValueError("Agregacao diaria Gold divergente dos pedidos")
    for row in tables["estoque_atual"]:
        if (row["produto_id"],) not in index["dim_produto"] or (row["data_id"],) not in index["dim_data"]:
            raise ValueError("Dimensao ausente no estoque Gold")
        if row["data_id"] != int(row["observado_em"].astimezone(zone).strftime("%Y%m%d")):
            raise ValueError("Data de observacao do estoque divergente")
    return {"orders_without_items": sum(row["quantidade_linhas"] == 0 for row in tables["fato_pedido"]),
            "unknown_customers": sum(row["is_unknown"] for row in tables["dim_cliente"]),
            "unknown_products": sum(row["is_unknown"] for row in tables["dim_produto"]),
            "rows_with_flags": {name: sum(bool(row.get("quality_flags")) for row in rows) for name, rows in tables.items()}}


def schemas():
    import pyarrow as pa
    text, integer, money = pa.string(), pa.int64(), pa.decimal128(18, 2)
    stamp = pa.timestamp("us", tz="UTC")
    metadata = [("source_system", text), ("source_event_id", text), ("source_object", text), ("event_at", stamp),
                ("is_simulated", pa.bool_()), ("quality_flags", pa.list_(text))]
    dimensions = [("is_deleted", pa.bool_()), ("is_unknown", pa.bool_())] + metadata
    fields = {
        "dim_cliente": [("cliente_id", text), ("nome", text), ("email", text)] + dimensions,
        "dim_produto": [("produto_id", text), ("nome", text), ("preco_atual", money)] + dimensions,
        "dim_data": [("data_id", integer), ("data", pa.date32()), ("ano", integer), ("mes", integer), ("dia", integer),
                     ("trimestre", integer), ("dia_semana", integer), ("fim_semana", pa.bool_())],
        "fato_pedido": [("pedido_id", text), ("cliente_id", text), ("data_id", integer), ("pedido_em", stamp),
                        ("status", text), ("moeda", text), ("metodo_pagamento", text), ("valor_pedido", money),
                        ("quantidade_linhas", integer), ("quantidade_unidades", integer)] + metadata,
        "fato_item_pedido": [("pedido_id", text), ("numero_linha", integer), ("produto_id", text), ("cliente_id", text),
                             ("data_id", integer), ("status", text), ("moeda", text), ("quantidade", integer),
                             ("valor_unitario", money), ("valor_item", money)] + metadata,
        "vendas_diarias": [("data_id", integer), ("status", text), ("moeda", text), ("is_simulated", pa.bool_()),
                            ("quantidade_pedidos", integer), ("quantidade_unidades", integer),
                            ("valor_pedidos", money), ("ticket_medio", money)],
        "estoque_atual": [("produto_id", text), ("deposito_id", text), ("data_id", integer),
                           ("saldo_fisico", pa.decimal128(18, 4)), ("saldo_virtual", pa.decimal128(18, 4)),
                           ("observado_em", stamp)] + metadata,
    }
    return {name: pa.schema(columns) for name, columns in fields.items()}
