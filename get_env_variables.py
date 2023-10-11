import os

os.environ['envn'] ='DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'pyspark'

current = os.getcwd()

src_oltp = current + '/source/oltp'

src_olap = current + '/source/olap'

