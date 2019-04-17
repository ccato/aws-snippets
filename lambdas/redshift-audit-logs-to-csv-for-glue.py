import logging
import boto3
import gzip
import re
from urllib.parse import unquote
import os

outbucket = os.getenv('OUTBUCKET')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def tables_in_query(sql_str):
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = set()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                tok = tok.replace('\\n', '')
                result.add(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]

    return result


def process_file(in_file_path, out_file_path):
    with gzip.open(in_file_path, 'rb') as f:
        lines = ['timestamp;database;username;userid;schema;table\n']
        with open(out_file_path, 'w+') as g:
            # write header
            for line in f:
                # time stamp = everything from first quote up to first [ -1
                # db, user, pid, userid, xid = first [ to first ]
                # query = from LOG: to \n"
                line = str(line)
                if line.lower().find('select') > -1 and line.lower().find('from') > -1:
                    timestamp = line[3:line.find('[') - 1]
                    parts = line[line.find('[') + 2:line.find(']')].split()
                    tables = tables_in_query(line)
                    for table in tables:
                        schema_name = 'None'
                        table_name = table
                        if table.find('.') > -1:
                            schema_name, table_name = table.split('.')
                        # timestamp, dbname, username, userid, schemaname, tablename
                        try:
                            towrite = '"{}";"{}";"{}";"{}";"{}";"{}"'.format(
                                timestamp,
                                parts[0].split('=')[1],
                                parts[1].split('=')[1],
                                parts[3].split('=')[1],
                                schema_name,
                                table_name)
                            towrite = re.sub('[\'"\\\]', '', towrite)
                            lines.append("{}\n".format(towrite))
                        except:
                            continue
            g.writelines(lines)
            g.flush()
        g.close()
    f.close()


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    s3r = boto3.resource('s3')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote(record['s3']['object']['key'])
        filepath = key[:key.rfind('/')]
        in_file_name = key[key.rfind('/') + 1:]
        out_file_name = "{}.csv".format(in_file_name[:len(in_file_name) - 3])
        if 'useractivitylog' in key:
            # download and process
            logger.info("Downloading s3://{}/{} to /tmp/{}".format(bucket, key, in_file_name))
            s3.download_file(bucket, key, '/tmp/' + in_file_name)
            logger.info("Downloaded s3://{}/{} to /tmp/{}".format(bucket, key, in_file_name))
            process_file("/tmp/{}".format(in_file_name), '/tmp/{}'.format(out_file_name))
            logger.info('Processed file {} into csv file'.format(in_file_name))
            # upload to import bucket so glue can catalog it
            s3r.meta.client.upload_file('/tmp/{}'.format(out_file_name), outbucket, "{}/{}".format(filepath, out_file_name))
            logger.info('Uploaded file {} to bucket {}'.format(out_file_name, outbucket))
        else:
            logger.info("Skipping s3://{}/{}".format(bucket, key))
