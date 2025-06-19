import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from application.services.performance_service import PerformanceService


def render_performance_results(performance_service: PerformanceService) -> bool:
    """Renderiza tablas y gráficos con los datos de rendimiento actuales."""
    performance_data_dict = performance_service.get_current_performance_data()

    if not performance_data_dict or not performance_data_dict['database']:
        st.warning(
            "No hay datos de rendimiento disponibles. Ejecute las pruebas primero desde la pestaña 'Ejecutar Pruebas de Rendimiento'."
        )
        return False

    df = pd.DataFrame(performance_data_dict)
    df_valid = df[df['time_ms'] >= 0]

    if df_valid.empty:
        st.warning(
            "Todos los resultados de las pruebas de rendimiento fueron erróneos o no hay datos válidos."
        )
        return False

    st.subheader("Datos Crudos de Tiempos de Ejecución (ms)")
    st.dataframe(df)

    st.subheader("Resumen Estadístico (solo datos válidos)")
    st.dataframe(df_valid.groupby(['database', 'operation'])['time_ms'].describe())

    st.subheader("Gráficos Comparativos (solo datos válidos)")

    try:
        fig_bar, ax_bar = plt.subplots(figsize=(12, 7))
        pivot_df = df_valid.pivot(index='database', columns='operation', values='time_ms')
        pivot_df.plot(kind='bar', ax=ax_bar, width=0.8)

        ax_bar.set_title("Tiempo de Ejecución por Operación y Base de Datos", fontsize=16)
        ax_bar.set_ylabel("Tiempo (ms)", fontsize=12)
        ax_bar.set_xlabel("Base de Datos", fontsize=12)
        ax_bar.legend(title="Operaciones", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.xticks(rotation=0)
        plt.tight_layout()
        st.pyplot(fig_bar)
    except Exception as e:
        st.error(f"Error al generar gráfico de barras: {e}")

    try:
        fig_line, ax_line = plt.subplots(figsize=(12, 7))
        for db_name in df_valid['database'].unique():
            db_data = df_valid[df_valid['database'] == db_name]
            ax_line.plot(db_data['operation'], db_data['time_ms'], label=db_name, marker='o', linestyle='-')

        ax_line.set_title("Comparación de Rendimiento entre Bases de Datos", fontsize=16)
        ax_line.set_ylabel("Tiempo (ms)", fontsize=12)
        ax_line.set_xlabel("Operación", fontsize=12)
        ax_line.legend(title="Base de Datos")
        ax_line.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        st.pyplot(fig_line)
    except Exception as e:
        st.error(f"Error al generar gráfico de líneas: {e}")

    return True

def results_tab_view(performance_service: PerformanceService):
    st.header("Resultados de Rendimiento")

    if not render_performance_results(performance_service):
        if st.button("Limpiar datos de rendimiento (si existen)"):
            performance_service.clear_all_performance_data()
            st.rerun()
        return

    if st.button("Limpiar Datos de Rendimiento Mostrados"):
        performance_service.clear_all_performance_data()
        st.rerun()
