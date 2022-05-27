import asyncio
import configparser
import datetime
import json
import os.path
import pathlib
import pprint
import uuid
import aio_pika
import boto3
import click
import tqdm


if os.path.exists('cache.json'):
    with open('cache.json') as f:
        cache = json.load(f)
else:
    cache = {}


def save():
    with open('cache.json', 'w') as f:
        json.dump(cache, f)


secrets = configparser.ConfigParser()
secrets.read(".secret")

ONE_YEAR = 60 * 60 * 24 * 365
pp = pprint.PrettyPrinter(indent=2)

session = boto3.Session()
s3 = session.resource(
    "s3",
    endpoint_url=secrets['main']['s3_endpoint'],
    aws_access_key_id=secrets['main']['s3_key'],
    aws_secret_access_key=secrets['main']['s3_secret'],
)
s3_client = session.client(
    "s3",
    endpoint_url=secrets['main']['s3_endpoint'],
    aws_access_key_id=secrets['main']['s3_key'],
    aws_secret_access_key=secrets['main']['s3_secret'],
)
bucket = s3.Bucket("osago")


async def send_and_wait(data: bytes):
    connection = await aio_pika.connect_robust(
        secrets['main']['rabbit_url'] + '/?'
        "cafile=ssl/ca-cert.pem&"
        "keyfile=ssl/client-key.pem&"
        "certfile=ssl/client-cert.pem&"
        "no_verify_ssl=1",
    )

    async with connection:
        routing_key = "tasks"
        ch = await connection.channel()
        queue = await ch.declare_queue('results', durable=True)
        print('publish task')
        c = await ch.default_exchange.publish(
            aio_pika.Message(body=data),
            routing_key=routing_key,
        )
        print('wait result')
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body)
                    print('received result:\n', pp.pformat(data))
                    return

def fields_from_dir(p: pathlib.Path):
    fields = []
    print(f'process {p}')
    for file in tqdm.tqdm(list(p.iterdir())):
        if file.name.endswith('.jpg'):
            key = f'{p.name}/{uuid.uuid1().__str__()}'
            if str(file) in cache:
                url = cache[str(file)]
            else:
                bucket.upload_file(str(file), key)
                url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Key": key, "Bucket": bucket.name},
                    ExpiresIn=ONE_YEAR,
                )
                cache[str(file)] = url
            fields.append({
                'field_name': file.name.split('.')[0],
                'url': url
            })
    return fields


@click.command()
@click.option("--dir", help="Folder with example images")
@click.option("--doc_type", help="Document type", default="doc_type_1")
@click.option("--force/--no-force", is_flag=True, help="Force page execution", default=False)
def main(dir: str, doc_type: str, force: bool):
    p = pathlib.Path(dir)
    if not p.exists():
        raise FileNotFoundError(dir)
    fields = []
    print('uploading files')
    documents = []
    for doc in p.iterdir():
        if doc.is_dir():
            documents.append({
                'url': str(doc),
                'type': doc_type,
                'fields': fields_from_dir(doc)
            })
    if not documents:
        fields = fields_from_dir(p)
        documents.append({
            'url': str(p),
            'type': doc_type,
            'fields': fields
        })
    req = {
        'uuid': uuid.uuid1().__str__(),
        'force': force,
        'documents': documents
    }
    data = json.dumps(req, indent=2)
    print('send task:\n', data)
    beg = datetime.datetime.now()
    asyncio.run(send_and_wait(data.encode()))
    print(f'fields cnt: {len(fields)}')
    print(f'total time: {datetime.datetime.now() - beg}')


if __name__ == '__main__':
    try:
        main()
    finally:
        save()
