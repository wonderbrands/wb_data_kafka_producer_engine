# Kafka Producer Engine (wb_data_kafka_producer_engine)

Este módulo permite la integración de Odoo con **Apache Kafka** (específicamente optimizado para **AWS MSK** con autenticación IAM). Su función principal es interceptar las operaciones de creación, actualización y eliminación de registros en modelos específicos y enviar esos cambios a tópicos de Kafka de forma asíncrona.

## Características Principales

- **Sincronización Automática**: Escucha eventos `create`, `write` y `unlink` de los modelos configurados.
- **Procesamiento Asíncrono**: Utiliza un pool de hilos (`ThreadPoolExecutor`) para evitar bloqueos en la interfaz de Odoo durante la comunicación con Kafka.
- **Configuración por Modelo**: Permite elegir qué modelos de Odoo se deben seguir.
- **Formatos de Datos**:
  - `API Like`: Formato basado en los valores de los campos de Odoo (resuelve relaciones Many2one a etiquetas legibles).
  - `Schema Like`: Formato basado en la estructura directa de la base de datos PostgreSQL.
- **Dumps Manuales**: Permite enviar registros existentes a Kafka basándose en una fecha de creación (`since`).
- **Gestión de Errores**: Registra el estado de cada mensaje (`Sent`, `Failed`, `Pending`) y permite reintentos manuales.
- **Autenticación AWS MSK**: Integrado con AWS IAM para conexiones seguras sin necesidad de contraseñas estáticas en texto plano.
- **API de Inspección**: Endpoint para consultar la estructura de campos de cualquier modelo.

## Requisitos Previos

### Dependencias de Python

Es necesario instalar las librerías listadas en `requirements.txt`. Puedes instalarlas usando:

```bash
pip install -r requirements.txt
```

Librerías clave:
- `kafka-python`: Cliente de Kafka.
- `aws-msk-iam-sasl-signer-python`: Para autenticación IAM con AWS MSK.
- `boto3`: SDK de AWS.

## Instalación y Configuración

### 1. Instalar el módulo en Odoo

1. Copia la carpeta `wb_data_kafka_producer_engine` a tu directorio de `addons`.
2. Actualiza la lista de aplicaciones en Odoo.
3. Busca "Kafka Producer Engine" e instálalo.

### 2. Configuración en la Interfaz

Una vez instalado, aparecerá un nuevo menú llamado **Kafka**.

1. **Configuración Global (Settings)**:
   - Ve a **Kafka > Settings** (o a Configuración General de Odoo).
   - Busca la sección **Kafka & AWS MSK Configuration**.
   - Ingresa los siguientes valores:
     - **Kafka Broker Servers**: Lista separada por comas de tus brokers (ej. `b-1.msk...:9098,b-2.msk...:9098`).
     - **AWS Region**: La región de tu clúster (ej. `us-east-1`).
     - **AWS Access Key**: ID de llave de acceso de AWS.
     - **AWS Secret Key**: Llave secreta de AWS.

2. **Followed Models**: Define qué modelos quieres monitorear (ej. `res.partner`, `sale.order`).
   - Activa `API Like` o `Schema Like` según tu necesidad.
   - Opcionalmente, puedes definir servidores bootstrap específicos por modelo si difieren del global.

3. **Kafka Message Handler**: Aquí puedes ver el historial de mensajes enviados, su contenido JSON y su estado de entrega.

## Funcionamiento Técnico

### Intercepción de Datos
El módulo hereda del modelo `base` de Odoo, lo que le permite interceptar llamadas a los métodos estándar de persistencia. Solo actúa si el modelo actual está presente en la configuración de "Followed Models".

### Envío Asíncrono
Cuando ocurre un cambio, el módulo prepara el mensaje JSON y lo pasa a un hilo secundario. Este hilo crea un registro en `kafka.message.handler` y luego intenta enviarlo al broker de Kafka. Si el envío falla, el registro queda marcado como `failed` con el error correspondiente.

### Endpoint de Metadatos
El módulo expone una ruta GET para inspeccionar modelos:
`GET /api/model_details?model=res.partner&data_like=api_like`

Retorna un JSON con los campos y sus tipos de datos.

## Estructura del Módulo

- `models/followed_models.py`: Configuración de modelos a seguir.
- `models/kafka_message_handler.py`: Lógica de conexión a Kafka y envío de mensajes.
- `models/dump.py`: Lógica para exportación masiva de datos históricos.
- `methods/methods.py`: Mixin que sobrescribe los métodos `create`, `write` y `unlink`.
- `controllers/model_details.py`: Endpoint API para detalles de esquemas.

## Notas de Seguridad
- Las credenciales de AWS se almacenan de forma segura en los parámetros del sistema de Odoo (`ir.config_parameter`).
- Se recomienda usar roles de IAM con permisos mínimos necesarios (Publish en los tópicos correspondientes).
