# moviesTFM
Proyecto Fin de Master Kschool
# MODULOS DEL PROYECTO
  # batch-process: 
  modulo spark que se encarga de la carga de ficheros CSV a HDFS, procesamiento de ficheros guardados en HDFS y guardado en MongoDB
  # common-module: 
  modulo con las utilidades de guardado en HDFS, y con las operaciones CRUD para MongoDB
  # documentacion: 
  ppt con la informacion del proyecto.
  # druid: 
  ficheros de carga del datasource, y un ejemplo de una query TopN
  # script: 
  scripts para parar y arrancar los servicios utilizados. Arranque de cada uno de los procesos spark utilizados.
  # simulator-data: 
  modulo usado en la simulacion de streaming.
  # streaming-process: 
  modulo spark encargado de recibir el streaming de informacion y guardalo en HDFS para luego usarlo por el proceso batch. Tambien envia la informacion a Druid para la explotacion del dato.
