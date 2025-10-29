from flask import Flask, render_template, Response, request, redirect, url_for
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib
matplotlib.use('Agg') # Usa um backend não-interativo

# Importe SEUS módulos
import data_loader
import analysis_core
import openf1_api # Módulo da OpenF1 (telemetria)

app = Flask(__name__)

# --- Cache de Dados (Para sua análise do CSV) ---
try:
    print("Carregando e limpando dados do position.csv...")
    df_global = data_loader.get_cleaned_data()
    df_pilotos_global = analysis_core.get_driver_performance(df_global)
    print("Dados locais (position.csv) prontos.")
except Exception as e:
    print(f"Aviso: Não foi possível carregar 'position.csv'. A análise local está desativada. Erro: {e}")
    df_global = None
    df_pilotos_global = None


@app.route('/')
def index():
    """
    Página inicial (Dashboard)
    """
    return render_template('index.html')

# --- Rotas para Análise do CSV Local ---

@app.route('/analysis-csv')
def analysis_csv():
    """
    Página dedicada à SUA análise do 'position.csv'.
    """
    if df_pilotos_global is None:
        return "Erro: Dados de análise do 'position.csv' não puderam ser carregados.", 500
        
    return render_template('analysis_csv.html')

@app.route('/plot/driver_performance.png')
def plot_driver_performance():
    """
    Endpoint que gera o gráfico da SUA análise (position.csv).
    """
    if df_pilotos_global is None:
        return "Erro: Dados de análise não carregados.", 500

    fig = analysis_core.plot_driver_performance_grid(df_pilotos_global)
    
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

# --- Rotas para Análise de Telemetria (OpenF1) ---

@app.route('/telemetry', methods=['GET', 'POST'])
def telemetry():
    """
    Página com formulário e exibição de telemetria.
    """
    if request.method == 'POST':
        # Pega os dados do formulário
        year = request.form.get('year')
        location = request.form.get('location')
        
        # Redireciona para a mesma página com os parâmetros na URL
        return redirect(url_for('telemetry', year=year, location=location))

    # Se for GET, verifica se há parâmetros na URL
    year = request.args.get('year')
    location = request.args.get('location')
    plot_urls = {}

    if year and location:
        # Se houver parâmetros, gera as URLs dos gráficos
        plot_urls = {
            'position': url_for('plot_telemetry_position', year=year, location=location),
            'overtakes': url_for('plot_telemetry_overtakes', year=year, location=location)
        }

    return render_template('telemetry.html', plot_urls=plot_urls, year=year, location=location)


@app.route('/plot/telemetry/position.png')
def plot_telemetry_position():
    """
    Endpoint que gera o gráfico de posições da OpenF1.
    """
    year = request.args.get('year')
    location = request.args.get('location')

    if not year or not location:
        return "Erro: Ano e Localização são necessários.", 400

    try:
        fig = openf1_api.get_position_plot(year=int(year), location=location)
        if fig is None:
             return f"Erro: Não foi possível gerar o gráfico. Dados não encontrados para {location} {year}?", 404

        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
    except Exception as e:
        print(f"Erro ao gerar gráfico de posição: {e}")
        return f"Erro interno ao gerar gráfico: {e}", 500

@app.route('/plot/telemetry/overtakes.png')
def plot_telemetry_overtakes():
    """
    Endpoint que gera o gráfico de ultrapassagens da OpenF1.
    """
    year = request.args.get('year')
    location = request.args.get('location')

    if not year or not location:
        return "Erro: Ano e Localização são necessários.", 400

    try:
        fig = openf1_api.get_overtakes_plot(year=int(year), location=location)
        if fig is None:
             return f"Erro: Não foi possível gerar o gráfico. Dados não encontrados para {location} {year}?", 404
             
        output = io.BytesIO()
        FigureCanvas(fig).print_png(output)
        return Response(output.getvalue(), mimetype='image/png')
    except Exception as e:
        print(f"Erro ao gerar gráfico de ultrapassagem: {e}")
        return f"Erro interno ao gerar gráfico: {e}", 500


if __name__ == '__main__':
    app.run(debug=True)

