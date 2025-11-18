# Este es el contenido para el archivo: main.py

from datetime import date
import peewee

# 1. Importar la clase gestora y los modelos
try:
    from gestionar_obras import GestionarObra
    from modelo_orm import (
        db, Etapa, TipoContratacion, Empresa, 
        FuenteFinanciamiento, Obra
    )
except ImportError as e:
    print(f"✗ Error: No se pudo importar un módulo. {e}")
    print("  Asegúrate de que 'modelo_orm.py' y 'gestionar_obras.py' estén en la misma carpeta.")
    exit()

def ejecutar_proceso_completo():
    """
    Función principal que ejecuta todos los pasos del TP.
    """
    print("--- INICIANDO TRABAJO PRÁCTICO FINAL ---")
    
    try:
        # --- PASO PREVIO: Conectar, Mapear y Cargar Datos (Puntos 2, 3, 4) ---
        print("\n--- PASO PREVIO: Cargando datos iniciales del CSV ---")
        
        # (b) Conectar a la BD [cite: 25]
        GestionarObra.conectar_db()
        
        # (c) Mapear ORM (crear tablas) [cite: 26]
        GestionarObra.mapear_orm()
        
        # (a) Extraer datos del CSV [cite: 24]
        GestionarObra.extraer_datos()
        
        # (d) Limpiar datos del DataFrame [cite: 27]
        GestionarObra.limpiar_datos()
        
        # (e) Cargar datos del DataFrame en la BD [cite: 28]
        # (Se ejecuta solo si la tabla 'obra' está vacía para evitar duplicados)
        if Obra.select().count() == 0:
            GestionarObra.cargar_datos()
        else:
            print("✓ (e) La base de datos ya contenía datos. Se omite la carga inicial.")

        
        # --- Punto 6: Crear nuevas instancias de Obra ---
        # Se deben crear dos instancias como mínimo 
        print("\n--- Punto 6: Creación de (mínimo) 2 nuevas obras ---")
        
        print("\n[Creando Obra 1...]")
        obra_1 = GestionarObra.nueva_obra()
        
        print("\n[Creando Obra 2...]")
        obra_2 = GestionarObra.nueva_obra()

        if not obra_1 or not obra_2:
            print("✗ Error: No se pudieron crear ambas obras. Abortando.")
            return

        # --- Puntos 7 al 16: Gestionar Ciclo de Vida ---
        print("\n--- Puntos 7-16: Gestionando ciclo de vida de OBRA 1 ---")
        
        # Usaremos el helper _buscar_fk de la clase para validar entradas
        buscar_fk = GestionarObra._buscar_fk

        # Punto 9: Iniciar Contratación 
        print("\n[Punto 9] Iniciando contratación para Obra 1...")
        tipo_contrato_1 = buscar_fk(TipoContratacion)
        nro_contrato_1 = input("  Ingrese Nro de Contratación: ")
        obra_1.iniciar_contratacion(tipo_contrato_1, nro_contrato_1)
        # .save() se llama dentro del método en 'modelo_orm.py'

        # Punto 10: Adjudicar Obra 
        print("\n[Punto 10] Adjudicando Obra 1...")
        empresa_1 = buscar_fk(Empresa)
        nro_exp_1 = input("  Ingrese Nro de Expediente: ")
        obra_1.adjudicar_obra(empresa_1, nro_exp_1)

        # Punto 11: Iniciar Obra 
        print("\n[Punto 11] Iniciando Obra 1...")
        destacada_1 = input("¿Es obra destacada? (SI/NO): ").strip().upper()
        if destacada_1 not in ["SI", "NO"]:
            destacada_1 = "NO"  # Valor por defecto
        fecha_inicio_1 = date.today()
        fecha_fin_1 = date(2026, 12, 31)
        fuente_finan_1 = buscar_fk(FuenteFinanciamiento)
        mano_obra_1 = int(input("  Ingrese cantidad de mano de obra: "))
        obra_1.iniciar_obra(
            destacada=destacada_1,
            fecha_inicio=fecha_inicio_1,
            fecha_fin_inicial=fecha_fin_1,
            fuente_financiamiento=fuente_finan_1,
            mano_obra=mano_obra_1
        )

        # Punto 12: Actualizar Porcentaje 
        print("\n[Punto 12] Actualizando avance Obra 1...")
        obra_1.actualizar_porcentaje_avance(50)

        # Punto 13: Incrementar Plazo (Opcional) [cite: 57, 58]
        print("\n[Punto 13] Incrementando plazo Obra 1 (Opcional)...")
        obra_1.incrementar_plazo(6) # Suma 6 meses al plazo

        # Punto 14: Incrementar Mano Obra (Opcional) [cite: 59, 60]
        print("\n[Punto 14] Incrementando mano de obra Obra 1 (Opcional)...")
        obra_1.incrementar_mano_obra(20) # Suma 20 trabajadores

        # Punto 15: Finalizar Obra 
        print("\n[Punto 15] Finalizando Obra 1...")
        obra_1.finalizar_obra()


        # --- Gestionando OBRA 2 (Demostración de 'Rescindir') ---
        print("\n--- Puntos 7-16: Gestionando ciclo de vida de OBRA 2 ---")
        
        # (Saltamos al Punto 16 para esta obra)
        # Punto 16: Rescindir Obra 
        print("\n[Punto 16] Rescindiendo Obra 2...")
        obra_2.rescindir_obra()
        
        print("\n--- Ciclo de vida de nuevas obras, completado. ---")


        # --- Punto 17: Obtener Indicadores ---
        print("\n--- Punto 17: Obteniendo indicadores finales ---")
        GestionarObra.obtener_indicadores()

    except peewee.OperationalError as e:
        print(f"\n✗ ERROR DE BASE DE DATOS: {e}")
    except FileNotFoundError as e:
        print(f"\n✗ ERROR DE ARCHIVO: {e}")
    except Exception as e:
        print(f"\n✗ ERROR INESPERADO: {e}")
    finally:
        if not db.is_closed():
            db.close()
            print("\n--- FIN DEL PROYECTO ---")
            print("✓ Conexión a la base de datos cerrada.")

# --- Punto de Entrada Principal ---
if __name__ == "__main__":
    ejecutar_proceso_completo()