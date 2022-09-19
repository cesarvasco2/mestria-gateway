import sys
import json
import paho.mqtt.client as mqtt
import boto3
import time
from datetime import date, datetime
import calendar
#from operations import Operations
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='processador_entrada')
#configurações do broker:
Broker = 'message.hidroview.com.br'
PortaBroker = 1883 
Usuario = 'mestria_gateway'
Senha = 'UhFQ+^AG%6eL8MdzQ8ZW'
KeepAliveBroker = 60
TopicoSubscribe = 'mestria/#' #Topico que ira se inscrever
#Callback - conexao ao broker realizada
def on_connect(client, userdata, flags, rc):
    print('[STATUS] Conectado ao Broker. Resultado de conexao: {}'.format(str(rc)))
#faz subscribe automatico no topico
    client.subscribe(TopicoSubscribe)
#Callback - mensagem recebida do broker
def on_message(client, userdata, msg):
    MensagemRecebida = str(msg.payload.decode('utf-8') )
    #print("[MESAGEM RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+MensagemRecebida)
    dict_payload = json.loads(MensagemRecebida)
    #print(dict_payload['DATA'])
    lista_de_campos = [
        {'key':'id_dispositivo','type':'str','fields':'DEVID'},
        {'key':'entradas_4a20','type':'4a20','fields':('LEVEL',)},
        {'key':'rele','type':'int','fields':('PUMP1','PUMP2',)},
        {'key':'status','type':'int','fields':('PUMP1','PUMP2',)},
        {'key':'pulso','type':'m3','fields':('VOLUME1','VOLUME2',)},
        {'key':'ct_pulso','type':',m3','fields':('PERIOD1','PERIOD2',)},
        {'key':'horimetro','type':'horimetro','fields':('HOURMETER1','HOURMETER2',)},


    ]
    dia_semana = date.today()
    data_e_hora_atuais = datetime.now() 
    dict_save = {}
    dict_save['data_hora_dispositivo'] = data_e_hora_atuais.strftime('%Y-%m-%d %H:%M:%S')
    for campo in lista_de_campos:
        if isinstance(campo['fields'],tuple):
            elementos_do_campo = []
            if campo['type']=='4a20':
                for field in campo['fields']:
                    if field in dict_payload['DATA']:
                        if field != "0":
                            elementos_do_campo.append(str(float(dict_payload['DATA'][field])/100))
                        else:
                             elementos_do_campo.append(str(float(dict_payload['DATA'][field])))   
            elif campo['type']=='m3':
                for field in campo['fields']:
                    if field in dict_payload['DATA']:
                        if field != "0":
                            elementos_do_campo.append(str(float(dict_payload['DATA'][field])/1000)) 
                        else:    
                            elementos_do_campo.append(dict_payload['DATA'][field]) 
            elif campo['type']=='horimetro':
                for field in campo['fields']:
                    if field in dict_payload['DATA']:
                        if field != "0":
                            elementos_do_campo.append(str(float(dict_payload['DATA'][field])/10))  
                        else:  
                            elementos_do_campo.append(str(float(dict_payload['DATA'][field])))    
            elif campo['type']=='pulso':
                for field in campo['fields']:
                    if field in dict_payload['DATA']:
                        elementos_do_campo.append(int(dict_payload['DATA'][field]))                                     
            else:
                for field in campo['fields']:
                    if field in dict_payload['DATA']:
                        if campo['type'] == 'int':
                            elementos_do_campo.append(int(dict_payload['DATA'][field]))   
                        else:
                            elementos_do_campo.append(str(dict_payload['DATA'][field]))   
            dict_save[campo['key']] = elementos_do_campo
        else:
            if campo['type']=='int':
                if campo['fields'] in dict_payload['DATA']:
                    dict_save[campo['key']] = int(dict_payload['DATA'][campo['fields']])
            else:
                if campo['fields'] in dict_payload['DATA']:
                    dict_save[campo['key']] = str(dict_payload['DATA'][campo['fields']])
    dict_save['codigo_produto']= 19 
    dict_save['timestamp_dispositivo'] = int(datetime.now().timestamp())
    dict_save['timestamp_servidor'] = int(datetime.now().timestamp())
    dict_save['dia_sem'] = calendar.day_name[dia_semana.weekday()]
    #Remove chaves vazias do dicionario
    remover_vazio = []
    for chave, valor in dict_save.items():
        if dict_save[chave] == []:
            remover_vazio.append(chave)
    for item in remover_vazio:
        dict_save.pop(item)
    print(dict_save)  
    #queue.send_message(MessageBody=str(json.dumps(dict_save, ensure_ascii=False)))  
try:
    print('[STATUS] Inicializando MQTT...')
    #inicializa MQTT:
    client = mqtt.Client('', True, None, mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(Usuario, Senha)
    # the key steps here
    #context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # if you do not want to check the cert hostname, skip it
    # context.check_hostname = False
    #client.tls_set_context(context)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_forever()

except KeyboardInterrupt:
    print ('\nCtrl+C pressionado, encerrando aplicacao e saindo...')
    sys.exit(0)    