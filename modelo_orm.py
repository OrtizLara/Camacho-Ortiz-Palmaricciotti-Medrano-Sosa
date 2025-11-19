import peewee
import os #Para manipular rutas en este caso

#Detecta el archivo obras_urbanas.db
carpeta=os.path.dirname(os.path.abspath(__file__))
#Construye la ruta, asi evitamos duplicar la base de datos
ruta=os.path.join(carpeta, 'obras_urbanas.db')
#Definir la conexión a la base de datos
db = peewee.SqliteDatabase(ruta)

#CLASE BASE (BaseModel) 
class BaseModel(peewee.Model):
    """Clase base para todos los modelos ORM"""
    class Meta:
        database = db



#TABLAS DE CATÁLOGO - 

class Comuna(BaseModel):
    """Comunas de la Ciudad de Buenos Aires (1-15)"""
    numero = peewee.CharField(unique=True)  # "1", "2", "3", etc.
    
    class Meta:
        table_name = 'comunas'
    
    def __str__(self):
        return f"Comuna {self.numero}"


class Barrio(BaseModel):
    """Barrios de CABA - Cada barrio pertenece a UNA comuna"""
    nombre = peewee.CharField(unique=True)
    comuna = peewee.ForeignKeyField(Comuna, backref='barrios', null=True)
    
    class Meta:
        table_name = 'barrios'
    
    def __str__(self):
        return self.nombre


class TipoObra(BaseModel):
    """Tipos de obra: Hidráulica, Arquitectura, Urbanización, etc."""
    nombre = peewee.CharField(unique=True)
    
    class Meta:
        table_name = 'tipos_obra'
    
    def __str__(self):
        return self.nombre


class AreaResponsable(BaseModel):
    """Áreas/Ministerios responsables de las obras"""
    nombre = peewee.CharField(unique=True)
    
    class Meta:
        table_name = 'areas_responsables'
    
    def __str__(self):
        return self.nombre


class Empresa(BaseModel):
    """Empresas contratistas"""
    nombre = peewee.CharField(unique=True)
    cuit = peewee.CharField(null=True)  #Puede no estar disponible
    
    class Meta:
        table_name = 'empresas'
    
    def __str__(self):
        return self.nombre


class Etapa(BaseModel):
    """Etapas del ciclo de vida de una obra"""
    nombre = peewee.CharField(unique=True)
    #Ejemplos: "Proyecto", "Licitación", "En Ejecución", 
    #Finalizada", "Rescindida", etc.
    
    class Meta:
        table_name = 'etapas'
    
    def __str__(self):
        return self.nombre


class TipoContratacion(BaseModel):
    """Tipos de contratación: Licitación Pública, Directa, etc."""
    nombre = peewee.CharField(unique=True)
    
    class Meta:
        table_name = 'tipos_contratacion'
    
    def __str__(self):
        return self.nombre


class FuenteFinanciamiento(BaseModel):
    """Fuentes de financiamiento de las obras"""
    nombre = peewee.CharField(unique=True)
    
    class Meta:
        table_name = 'fuentes_financiamiento'
    
    def __str__(self):
        return self.nombre


#TABLA PRINCIPAL- OBRA (Normalizada con Foreign Keys)


class Obra(BaseModel):
    
    #IDENTIFICACIÓN
    nombre = peewee.CharField(index=True)
    descripcion = peewee.TextField(null=True)
    entorno = peewee.CharField(null=True)  # Contexto/entorno de la obra
    
    #RELACIONES (FK) 
    tipo_obra = peewee.ForeignKeyField(
        TipoObra, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    area_responsable = peewee.ForeignKeyField(
        AreaResponsable, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    barrio = peewee.ForeignKeyField(
        Barrio, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    #Nota: NO guardamos comuna directamente porque depende de barrio 
    #Se accede como: obra.barrio.comuna
    
    etapa = peewee.ForeignKeyField(
        Etapa, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    empresa = peewee.ForeignKeyField(
        Empresa, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    tipo_contratacion = peewee.ForeignKeyField(
        TipoContratacion, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    fuente_financiamiento = peewee.ForeignKeyField(
        FuenteFinanciamiento, 
        backref='obras', 
        null=True,
        on_delete='SET NULL'
    )
    
    #DATOS ECONÓMICOS
    monto_contrato = peewee.FloatField(null=True)
    
    #UBICACIÓN
    direccion = peewee.CharField(null=True)
    lat = peewee.FloatField(null=True)
    lng = peewee.FloatField(null=True)
    
    #FECHAS Y PLAZOS
    fecha_inicio = peewee.DateField(null=True)
    fecha_fin_inicial = peewee.DateField(null=True)
    plazo_meses = peewee.IntegerField(null=True)
    
    #SEGUIMIENTO 
    porcentaje_avance = peewee.FloatField(default=0)
    mano_obra = peewee.IntegerField(null=True)  # Cantidad de trabajadores
    
    # LICITACIÓN Y CONTRATACIÓN
    licitacion_oferta_empresa = peewee.CharField(null=True)
    licitacion_anio = peewee.IntegerField(null=True)
    nro_contratacion = peewee.CharField(null=True)
    nro_expediente = peewee.CharField(null=True)
    cuit_contratista = peewee.CharField(null=True)
    
    #CARACTERÍSTICAS
    destacada = peewee.CharField(null=True)  # "SI" / "NO"
    ba_elige = peewee.CharField(null=True)
    beneficiarios = peewee.CharField(null=True)
    compromiso = peewee.CharField(null=True)
    
    #RECURSOS MULTIMEDIA Y DOCUMENTOS 
    imagen_1 = peewee.CharField(null=True)
    imagen_2 = peewee.CharField(null=True)
    imagen_3 = peewee.CharField(null=True)
    imagen_4 = peewee.CharField(null=True)
    link_interno = peewee.CharField(null=True)
    pliego_descarga = peewee.CharField(null=True)
    estudio_ambiental_descarga = peewee.CharField(null=True)
    
    class Meta:
        table_name = 'obras'
    
    def __str__(self):
        return f"Obra: {self.nombre} ({self.etapa})"
    
    

    #MÉTODOS DE INSTANCIA - Gestión del ciclo de vida de la obra
    def nuevo_proyecto(self):
        """Inicia una nueva obra en etapa 'Proyecto'"""
        etapa_proyecto, _ = Etapa.get_or_create(nombre="Proyecto")
        self.etapa = etapa_proyecto
        self.save()
        print(f"Obra '{self.nombre}' iniciada en etapa Proyecto")
    
    def iniciar_contratacion(self, tipo_contratacion, nro_contratacion):
        """
        Inicia el proceso de licitación/contratación.
        
        Args:
            tipo_contratacion: Instancia de TipoContratacion
            nro_contratacion: String con el número de contratación
        """
        self.tipo_contratacion = tipo_contratacion
        self.nro_contratacion = nro_contratacion
        
        #Cambiar etapa a "Licitación" o "En Licitación"
        etapa_licitacion, _ = Etapa.get_or_create(nombre="En Licitación")
        self.etapa = etapa_licitacion
        self.save()
        print(f"Contratación iniciada: {tipo_contratacion.nombre} - Nro: {nro_contratacion}")
    
    def adjudicar_obra(self, empresa, nro_expediente):
        """
        Adjudica la obra a una empresa.
        
        Args:
            empresa: Instancia de Empresa
            nro_expediente: String con el número de expediente
        """
        self.empresa = empresa
        self.nro_expediente = nro_expediente
        
        etapa_adjudicada, _ = Etapa.get_or_create(nombre="Adjudicada")
        self.etapa = etapa_adjudicada
        self.save()
        print(f"Obra adjudicada a {empresa.nombre} - Exp: {nro_expediente}")
    
    def iniciar_obra(self, destacada, fecha_inicio, fecha_fin_inicial, 
                     fuente_financiamiento, mano_obra):
        """
        Inicia la ejecución de la obra.
        
        Args:
            destacada: "SI" o "NO"
            fecha_inicio: objeto date
            fecha_fin_inicial: objeto date
            fuente_financiamiento: Instancia de FuenteFinanciamiento
            mano_obra: int (cantidad de trabajadores)
        """
        self.destacada = destacada
        self.fecha_inicio = fecha_inicio
        self.fecha_fin_inicial = fecha_fin_inicial
        self.fuente_financiamiento = fuente_financiamiento
        self.mano_obra = mano_obra
        
        etapa_ejecucion, _ = Etapa.get_or_create(nombre="En Ejecución")
        self.etapa = etapa_ejecucion
        self.save()
        print(f" Obra iniciada el {fecha_inicio} con {mano_obra} trabajadores")
    
    def actualizar_porcentaje_avance(self, porcentaje):
        """
        Actualiza el porcentaje de avance de la obra.
        
        Args:
            porcentaje: float (0-100)
        """
        if 0 <= porcentaje <= 100:
            self.porcentaje_avance = porcentaje
            self.save()
            print(f" Avance actualizado: {porcentaje}%")
        else:
            print(" Error: El porcentaje debe estar entre 0 y 100")
    
    def incrementar_plazo(self, nuevos_meses):
        """
        Incrementa el plazo de la obra.
        
        Args:
            nuevos_meses: int (nuevo plazo total en meses)
        """
        if nuevos_meses >= 0:
            plazo_anterior = self.plazo_meses if self.plazo_meses is not None else 0
            self.plazo_meses = plazo_anterior + nuevos_meses
            self.save()
            print(f" Plazo incrementado de {plazo_anterior} a {nuevos_meses} meses")
        else :
            print("El plazo no puede ser negativo")
    
    def incrementar_mano_obra(self, nueva_cantidad):
        """
        Incrementa la cantidad de trabajadores.
        
        Args:
            nueva_cantidad: int (nueva cantidad total)
        """
        if nueva_cantidad >= 0:
            cantidad_anterior = self.mano_obra if self.mano_obra is not None else 0 
            """if self.mano_obra is not None:
                cantidad_anterior = self.mano_obra
                else:
                    cantidad_anterior=0"""
            self.mano_obra = cantidad_anterior + nueva_cantidad
            self.save()
            print(f" Mano de obra incrementada de {cantidad_anterior} a {self.mano_obra} trabajadores")
        else:
            print("No puede ser negativo")
    
    def finalizar_obra(self):
        """Marca la obra como finalizada con 100% de avance"""
        etapa_finalizada, _ = Etapa.get_or_create(nombre="Finalizada")
        self.etapa = etapa_finalizada
        self.porcentaje_avance = 100
        self.save()
        print(f" Obra '{self.nombre}' FINALIZADA exitosamente")
    
    def rescindir_obra(self):
        """Rescinde/cancela la obra"""
        etapa_rescindida, _ = Etapa.get_or_create(nombre="Rescindida")
        self.etapa = etapa_rescindida
        self.save()
        print(f" Obra '{self.nombre}' RESCINDIDA")