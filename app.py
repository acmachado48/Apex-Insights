from flask import Flask, render_template, Response, request, redirect, url_for
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

# Importe SEUS módulos
import data_loader
import analysis_core
import f1_api # O novo módulo!

app = Flask(__name__)

# --- Cache de Dados (Para sua análise do CSV) ---
print("Carregando e limpando dados do position.csv...")
df_global = data_loader.get_cleaned_data()
df_pilotos_global = analysis_core.get_driver_performance(df_global)
print("Dados locais prontos.")


@app.route('/')
def index():
    """
    Página inicial (Dashboard) - Pode mostrar infos gerais da API.
    """
    # Ex: Buscar o campeão do ano passado
    champion_data = f1_api.get_world_champion_by_year(2023)
    return render_template('index.html', champion=champion_data)

@app.route('/analysis')
def analysis():
    """
    Página dedicada à SUA análise do 'position.csv'.
    """
    # Este template pode ter uma tag <img src="/plot/driver_performance.png">
    return render_template('analysis.html')

@app.route('/plot/driver_performance.png')
def plot_driver_performance():
    """
    Endpoint que gera o gráfico da SUA análise.
    """
    fig = analysis_core.plot_driver_performance_grid(df_pilotos_global)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/search', methods=['GET', 'POST'])
def search():
    """
    Página com os formulários de pesquisa da API Ergast.
    """
    if request.method == 'POST':
        # Detecta qual formulário foi enviado
        search_type = request.form.get('search_type')
        
        if search_type == 'champion':
            year = request.form.get('year')
            result = f1_api.get_world_champion_by_year(year)
            return render_template('search.html', champion_result=result)
            
        elif search_type == 'fastest_lap':
            year = request.form.get('year')
            round_num = request.form.get('round')
            result = f1_api.get_fastest_lap_by_race(year, round_num)
            return render_template('search.html', lap_result=result)
            
    # Se for GET, apenas mostra a página de pesquisa
    return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=True)
