#%%
import geopandas as gpd
import pandas as pd
import numpy as np
import json

from threading import Thread

from flask import Flask, render_template
from tornado.ioloop import IOLoop

from bokeh.models import (CDSView, ColorBar, ColumnDataSource,
                          CustomJS, CustomJSFilter, Div,
                          GeoJSONDataSource, HoverTool, PreText,
                          LinearColorMapper, Slider, WheelZoomTool,
                          Arrow, VeeHead, Select, TextInput,
                          CheckboxGroup)
from bokeh.layouts import column, row, widgetbox, layout
from bokeh.palettes import brewer, Colorblind
from bokeh.plotting import figure, output_file, show, output_notebook
from bokeh.tile_providers import Vendors, get_provider
from bokeh.io import curdoc
from bokeh.events import Tap
from bokeh.server.server import Server
from bokeh.themes import Theme
from bokeh.embed import server_document


app = Flask(__name__)

# Définition de variables globales
displaySet = []
#impot par défaut
impot = 'TauxTH_' 
#palette
defaultPalette = brewer['RdYlGn'][7] #7 couleurs Vert-Jaune-Rouge
# gestion de la commune par défaut
dist = 10
ogCity = []
# années pour lesquelles on dispose des données
data_yr= [2016, 2017, 2018]

def bkapp(doc):
    def createDataSet():
        '''
            Charge les données d'entrées dans un dataFrame geopandas
            Données d'entrées :
                - shapefile contenant le traçé des communes au format geojson
                - fichier texte contenant les données aui nous interesse au format csv
            Tâches réalisées :
                - chargement des fichiers
                - reprojection de wgs84 vers webmercator
                - tri des données inutiles
                - calcul de données à partir des données existantes
                - assemblage des données dans un geodataframe
                - tri des NaN/inf            
            Sortie : 
                - un geoDataFrame
        '''       
        # Fichier contenant le tracé des communes (format geojson)
        city_shapefile = "DATA/communes-20190101.json"
        # Fichiers de données
        taxe_hab = "DATA/taux_taxe_habitation.xlsx"
        taxe_fon = "DATA/taux_taxe_fonciere.xlsx"

        ########## Gestion de la géométrie des communes ############    
        
        # import de la geometrie des communes
        df_shape = gpd.read_file(city_shapefile)
        # Suppression des colonnes  "wiki" et "surface", inutiles
        df_shape.drop(columns=["wikipedia", "surf_ha"],inplace=True)
        
        # reprojection en webmercator
        df_shape["geometry"] = df_shape["geometry"].to_crs("EPSG:3857")
        df_shape.crs = "EPSG:3857"


        ########## Gestion des stats sur les communes ############
        
        # Taxe habitation
        # Import des taux d'imposition par commune dans la dataframe
        dfTH = pd.read_excel(taxe_hab,sheet_name="COM",header=2,usecols="A:B,E:G", converters={'Code commune':str,'Code DEP':str})

        # Mise en forme des libelles des colonnes
        dfTH.columns = dfTH.columns.str.replace(' ','_')
        dfTH.columns = dfTH.columns.str.replace('Taux_communal_TH*','TauxTH').str.replace('Taux_communal_voté_TH*','TauxTH')

        # On crée le code INSEE en concatenant le code departement et commune
        # Le code Insee sera la clé commune entre les dataframe de géométrie et de data.
        # Création du code Insee dans une nouvelle colonne de la df
        dfTH["insee"] = dfTH["Code_DEP"] + dfTH["Code_commune"]
        # Suppression de la colonne code commune qui ne sert plus à rien
        dfTH.drop(columns=["Code_commune"], inplace=True)

        # On converti les valeurs non numériques de la colonnes TauxTH en NaN pour les filtrer
        dfTH["TauxTH_2018"] = pd.to_numeric(dfTH["TauxTH_2018"], errors='coerce')
        dfTH["TauxTH_2017"] = pd.to_numeric(dfTH["TauxTH_2017"], errors='coerce')

        # Taxe foncière
        dfTF = pd.read_excel(taxe_fon,sheet_name="COM",header=2,usecols="A:B,D:F", converters={'Code commune':str,'Code DEP':str})
        dfTF.columns = dfTF.columns.str.replace(' ','_')
        dfTF.columns = dfTF.columns.str.replace('Taux_communal_TFB*','TauxTF').str.replace('Taux_communal_voté_TFB*','TauxTF')    
        dfTF["insee"] = dfTF["Code_DEP"] + dfTF["Code_commune"]
    
        dfTF.drop(columns=["Code_commune"], inplace=True)
        dfTF.drop(columns=["Code_DEP"], inplace=True)

        # On converti les valeurs non numériques de la colonnes TauxTH en NaN pour les filtrer
        dfTF["TauxTF_2018"] = pd.to_numeric(dfTF["TauxTF_2018"], errors='coerce')
        dfTF["TauxTF_2017"] = pd.to_numeric(dfTF["TauxTF_2017"], errors='coerce')

        # Assemblage de la géométrie et des taux d'imposition.
        dataCities = pd.merge(df_shape,dfTH, left_on="insee",right_on="insee", how = 'left')
        dataCities = pd.merge(dataCities,dfTF, left_on="insee",right_on="insee", how = 'left')

        return dataCities

    ### Fonctions de traitement ###

    def select_data(df, ogCity, dist):
        """ 
            Fonction qui permet de sélectionner les données à afficher
            Sélectionnées en fonction de la distance autour de la ville :
            On prend toutes les villes dont le contour est intersecté
            par le contour de la ville originale augmenté de dist
            Entrées :
                - df : dataframe qui contient toutes les données
                - ogCity : extract de la commune sélectionnée
                - dist : distance à l'origine (l'unité dépend du CRS, le EPSG:3857 est en m)
            Sortie :
                - dataFrame ne contenant que les données retenues
        """       
        # La fonction renvoie les communes qui sont intersectées par le cercle de centre ogCity
        # et de rayon dist*1000 (le rayon est entré en km)    
        return df[df.intersects(other=ogCity.geometry.buffer(dist*1000))]


    def create_choropleth(displaySet, displayParam, palette, ogCity):
        """ 
            Fonction qui met à jour la coloration des communes affichées
            Entrées :
                - displaySet : dataFrame contenant les données affichées
                - displayParam : paramètres que l'on souhaite aficher
                - palette : liste de couleurs (identique à celle de la choroplèthe)
                - ogCity : extract de la commune sélectionnée
            Sorties : 
                - Figure contenant la carte.
        """
        # On récupère les limites géographiques pour initialiser la carte
        displayBounds = displaySet.total_bounds

        # conversion du tracé des communes en json pour interpretation par Bokeh 
        geosource = GeoJSONDataSource(geojson = displaySet.to_json())

        # Creation de la figure de la carte
        choroPlot = figure(title = 'Taux ' + select_imp.value + " " + str(slider_yr.value),
                    x_range=(displayBounds[0], displayBounds[2]),
                    y_range=(displayBounds[1], displayBounds[3]), 
                    x_axis_type="mercator",
                    y_axis_type="mercator",
                    plot_height = 500 ,
                    plot_width = 850, 
                    sizing_mode = "scale_width",
                    toolbar_location = 'below',
                    tools = "pan, wheel_zoom, box_zoom, reset",
                    x_axis_location=None, 
                    y_axis_location=None
                )
        
        choroPlot.xgrid.grid_line_color = None
        choroPlot.ygrid.grid_line_color = None

        # Ajout d'un évèmenent de type clic, pour sélectionnr la commune de référence 
        choroPlot.on_event(Tap, update_loc)

        #outil de zoom molette activé par défaut
        choroPlot.toolbar.active_scroll = choroPlot.select_one(WheelZoomTool) 

        # ajout du fond de carte
        tile_provider = get_provider(Vendors.CARTODBPOSITRON)
        choroPlot.add_tile(tile_provider)

        # On détermine les vals min et max du jeu de test pour la gestion des couleurs
        mini = displaySet[displayParam].min()
        maxi = displaySet[displayParam].max()
    
        # Création d'une échelle de couleur évoulant linéairement avec le paramètre à afficher
        color_mapper = LinearColorMapper(palette = defaultPalette, 
                                    low = mini,                              
                                    high = maxi, 
                                    nan_color = '#808080'
                                    )

        # Ajout du tracé des communes sur la carte
        citiesPatch = choroPlot.patches('xs','ys', 
                        source = geosource,
                        line_color = 'gray', 
                        line_width = 0.25, 
                        fill_alpha = 0.5,
                        fill_color = {'field' : displayParam , 'transform': color_mapper}
                        )   

        # création de la legende # 
        color_bar = ColorBar(color_mapper=color_mapper,
                        label_standoff=8,
                        location=(0,0),
                        orientation='vertical'
                        ) 
        choroPlot.add_layout(color_bar, 'right')  

        # ajout d'une flèche sur la commune de reférence
        start = ogCity.geometry.centroid
        pin_point = Arrow(end=VeeHead(size=15), 
                            line_color="red",
                            x_start=start.x, 
                            y_start=start.y, 
                            x_end=start.x, 
                            y_end=start.y - 0.001
                        )
        choroPlot.add_layout(pin_point)

        #  Ajout d'un tooltip au survol de la carte
        choroHover = HoverTool(renderers = [citiesPatch],
                            tooltips = [('Commune','@nom'),
                                    (displayParam, '@' + displayParam)]
                            )                 
        toolTip = choroPlot.add_tools(choroHover)

        return choroPlot
            

    # Fonction de création de l'histogramme
    def createHisto(displaySet, displayParam, palette, ogCity):
        """
            L'histogramme permet de visualiser la répartition des taux des communes affichées
            Entrées :
                - displaySet : dataFrame contenant les données affichées
                - displayParam : paramètres que l'on souhaite aficher
                - palette : liste de couleurs (identique à celle de la choroplèthe)
                - ogCity : extract de la commune sélectionnée
            Sorties : 
                - figure contenant l'histogramme.
        """

        # On crée autant de regroupement que de couleurs passées à la fct°
        nBins = len(palette)
        # Calcul de l'histogramme
        hist, edges = np.histogram(displaySet[displayParam].dropna(), bins=nBins)
        # Nombre de lignes dans displaySet (vectorisé pour passage à datasource)
        total = displaySet[displayParam].size * np.ones(nBins, np.int8)
        # Normalisation de l'histogramme (affichage en % du total d'éléments)
        hist_pct = 100*hist/total[0]

        # Calcul de l'étendue l'échelle verticale
        hmax = max(hist_pct)*1.1
        hmin= -0.1*hmax

        # Calcul de la moyenne et médiane de l'échantillon
        mean = displaySet[displayParam].mean()
        med =  displaySet[displayParam].quantile(0.5)

        # Création de la figure contenant l'histogramme
        histoPlot = figure(title = 'Répartition du taux de ' + select_imp.value + " " + str(slider_yr.value),           
                    y_range=(hmin, hmax), 
                    plot_height = 300 ,
                    plot_width = 400,
                    sizing_mode = "scale_width",            
                    y_axis_location='right',
                    toolbar_location=None                
                    )
        histoPlot.xgrid.grid_line_color = None  
        histoPlot.xaxis.axis_label = displayParam
        histoPlot.xaxis.axis_line_color = None
        histoPlot.ygrid.grid_line_color = "white"
        histoPlot.yaxis.axis_label = '% de l\'échantillon'
        histoPlot.yaxis.axis_line_color = None 

        # Source de données
        data = dict(right=edges[1:],
                    left=edges[:-1] ,           
                    top=hist_pct,
                    nb=hist,
                    total=total,
                    color=palette
                )
        histoSource = ColumnDataSource(data=data)

        # Tracé de l'histogramme
        histoDraw = histoPlot.quad(bottom=0,
                                    left="left",
                                    right="right",
                                    top="top",
                                    fill_color = "color",
                                    line_color=None,
                                    source=histoSource)

        #  Ajout d'un tooltip au survol de la carte
        histoHover = HoverTool(renderers = [histoDraw],
                            mode = "vline",
                            tooltips = [('Taille', '@nb'),
                                        ('Fourchette', '@left - '+'@right'),                                
                                        ]
                        )                 
        histoTooltip = histoPlot.add_tools(histoHover)

        # Ajout d'un repère vertical pour la commune sélectionnée
        if ~np.isnan(ogCity[displayParam]) :   
            ogCityDraw = histoPlot.quad(bottom=hmin,
                                        top=hmax,
                                        left=ogCity[displayParam] - 0.05,
                                        right=ogCity[displayParam] + 0.05,                                
                                        fill_color = "pink",
                                        line_color= None,
                                        legend_label=ogCity["nom"] + ' (' + ogCity["Code_DEP"]+')'
                                    )

            #  Ajout d'un tooltip au survol de la commune d'orginie
            displayName = ogCity["nom"]+" ("+ogCity["Code_DEP"]+")"
            ogCityHover = HoverTool(renderers = [ogCityDraw],
                                mode = "vline",
                                tooltips = [('Commune sélectionnée', displayName),
                                            (displayParam, str(ogCity[displayParam])),                                
                                            ]
                            )                 
            histoTooltip = histoPlot.add_tools(ogCityHover)

        # Ajout d'un repère vertical pour la moyenne de l'échantillon    
        ogCityDraw = histoPlot.quad(bottom=hmin,
                                    top=hmax,
                                    left=mean - 0.05,
                                    right=mean + 0.05,                                
                                    fill_color = "blue",
                                    line_color= None,
                                    legend_label="Moyenne ")  

        # Ajout d'un repère vertical pour la mediane de l'échantillon    
        ogCityDraw = histoPlot.quad(bottom=hmin,
                                    top=hmax,
                                    left=med - 0.05,
                                    right=med + 0.05,                                
                                    fill_color = "purple",
                                    line_color= None,
                                    legend_label="Mediane ")
        
        # On rend la légende interactive
        histoPlot.legend.click_policy="hide"
        # On oriente horizontalement la légende
        histoPlot.legend.orientation="vertical"
        # Réduction de la police
        histoPlot.legend.label_text_font_size = "8px"
        # On place la légende hors de la zone de tracé
        histoLegend = histoPlot.legend[0]
        histoPlot.legend[0] = None
        histoPlot.add_layout(histoLegend, 'right')
        
        return histoPlot

    def create_info(displaySet, displayParam, ogCity):
        """
            Affiche un panneau textuel contenant des infomations sur le jeu de données
            affiché et la commune sélectionnée.
            Entrées :
                - displaySet : dataFrame contenant les données affichées
                - displayParam : paramètres que l'on souhaite aficher
                - ogCity : extract de la commune sélectionnée
            Sorties : 
                - figure contenant le texte à afficher.
        """
        stats = displaySet[displayParam].dropna().describe(percentiles=[0.5]).round(decimals=2)
        stats = stats[stats.index != 'count'] #On supprime la variable "count" deja affichée

        # Modification de l'intitulé des colonnes 
        stats.columns = ["Taux " + str(elt) for elt in data_yr]     
        
        # Creation du texte
        infoText = [f"<b>Communes affichées</b> : {len(displaySet)}",
                    f"<b>Commune sélectionnée</b> : {ogCity['nom']} ({ogCity['Code_DEP']})",
                    "</br><b>Statistiques</b> : " + select_imp.value 
                ]
                
        return [Div(text="</br>".join(infoText)), PreText(text=str(stats))]


    def update_layout(displaySet, displayParam, ogCity, palette):
        """
            Fonction permettant de mettre à jour toutes les figures du layout
            Entrées :
                - displaySet : dataFrame contenant les données affichées
                - displayParam : paramètres que l'on souhaite aficher
                - ogCity : extract de la commune sélectionnée
                - palette : liste de couleurs
            Sorties : 
                - rien
        """

        # Mise à jour de la chroplèthe
        appLayout.children[0].children[0] =create_choropleth(displaySet, displayParam, palette, ogCity)

        # Mise à jour de l'histogramme
        appLayout.children[1].children[0] = createHisto(displaySet, displayParam, palette, ogCity)  
    
        # Mise à jour des infos    
        appLayout.children[1].children[1:] = create_info(displaySet, infoParam, ogCity)

    def create_displayParam(impot='TauxTH_', year=2018):
        """
            Fonction qui retourne le paramètre à afficher dans la dataframe, à partir de l'impôt 
            et de l'année désirée.
            Entrées :
                - impot : l'impot que l'ont souhaite afficher (str)
                - année (int) : l'année que l'on souhait afficher (int)
            Sortie :
                - displayParam : le paramètre d'affichge (str)
            """

        return impot + str(year)


    ### Fonction callback ###

    # Callback fonction (met à jour le graphique)
    def update_yr(attr, old, new):    
        """
            Fonction callback appelée au changement du slider des années.
            Permet de modifier l'année du taux affiché.        
        """

        # Création du paramètre à afficher en fonction de l'année sélectionnée :
        displayParam = create_displayParam(impot,slider_yr.value)

        #   Mise à jour du layout
        update_layout(displaySet, displayParam, ogCity, defaultPalette)

    def update_dst(attr, old, new):

        """
            Fonction callback appelée au changement de la distance d'affichage
            Modifie le jeu de données afiché et recalcule les couleurs de nouveau jeu
        """
        # Utilisation de variables globales (nécessaire car utilisée par plusieurs callback)
        global displaySet
    
        # Création du paramètre à afficher en fonction de l'année sélectionnée :
        displayParam = create_displayParam(impot,slider_yr.value)

        # Mise à jour du jeu d'affichage
        displaySet = select_data(dataCities, ogCity, slider_dst.value)

        #  Mise à jour du layout
        update_layout(displaySet, displayParam, ogCity, defaultPalette)


    def update_loc(event):
        """
            Fonction callback activé au clic sur la map
            Permet de changer la commune sélectionnée
            Maj la carte avec la nouvelle commune de référence
        """
        # Utilisation de variables globales (nécessaire car utilisée par plusieurs callback)
        global ogCity
        global displaySet

        ### Identification de la commune sous le point cliqué ###
        
        # Création d'un objet shapely de type Point aux coords du clic :
        clicPoint = gpd.points_from_xy([event.x], [event.y])    
        # Creation d'une geoserie contenant le point :
        # Rem : utilisation de iloc[0] pour ne pas avoir d'index
        # cf issue https://github.com/geopandas/geopandas/issues/317 
        pointSerie = gpd.GeoSeries(crs='epsg:3857', data=clicPoint).iloc[0]
        # On recherche la commune qui contient le point :
        clicCity = dataCities[dataCities.contains(other=pointSerie)]     
        # On vérifie avant maj que le clic a bien retourné une géométrie
        if not clicCity.empty :        
            ogCity = clicCity.iloc[0]

        ### Mise à jour de la carte avec la commune cliquée pour référence ###

        # Calcul du nouveau jeu de données à afficher
        displaySet = select_data(dataCities, ogCity, slider_dst.value)
        # Création du paramètre à afficher en fonction de l'année sélectionnée :
        displayParam = create_displayParam(impot,slider_yr.value)
        #  Mise à jour du layout
        update_layout(displaySet, displayParam, ogCity, defaultPalette)
        
    def update_colormap(attr,old,new):
        """
            Change la palette de couleurs utilisée à l'action sur le toggle idoine
        """
        global defaultPalette

        # Création du paramètre à afficher en fonction de l'année sélectionnée :
        displayParam = create_displayParam(impot,slider_yr.value)
        
        if len(new) > 0:
            print('Mode Daltonien')
            defaultPalette = Colorblind[7] 
        else:
            print('Mode Normal')
            defaultPalette = brewer['RdYlGn'][7] 
        
        #  Mise à jour du layout
        update_layout(displaySet, displayParam, ogCity, defaultPalette)
        
    def update_impot(attr, old, new):
        global impot

        dict_imp = {"Taxe d'habitation" : "TauxTH_", 
                    "Taxe foncière" : "TauxTF_"
                }
        impot = dict_imp[new]

        # Création du paramètre à afficher en fonction de l'année sélectionnée :
        displayParam = create_displayParam(impot,slider_yr.value)

        #   Mise à jour du layout
        update_layout(displaySet, displayParam, ogCity, defaultPalette)

        
    #%%
    # Chargement du jeu de test
    try:
        dataCities = gpd.read_file("DATA/dataCities.json")
    except:
        print("fichier dataCities.json non trouvé, génération en cours")
        dataCities = createDataSet()
        # Sauvegarde du dataSet
        dataCities.to_file("DATA/dataCities.json", driver='GeoJSON')

    # %%

    ### Main Code ####

    ### Constrution de la carte et légende ####

    global displaySet
    global impot
    global defaultPalette
    global ogCity

    # Paramètres par défaut    
    ogCity = dataCities[dataCities["nom"]=='Paris'].iloc[0] # Paris sélectionnée par défaut
    # paramètre affiché par défaut = taxe d'habitation la plus récente
    defaultParam = impot + str(data_yr[-1])
    infoParam = [impot + str(elt) for elt in data_yr]

    # Création du set de donnée à afficher 
    displaySet = select_data(dataCities,ogCity,dist)


    ### Construction du front-end ###

    # Ajout d'un slider pour choisir l'année 
    slider_yr = Slider(title = 'Année',
                        start = data_yr[0], 
                        end = data_yr[-1], 
                        step = 1, 
                        value = data_yr[-1],
                        default_size = 250
                        )
    slider_yr.on_change('value', update_yr)

    # Ajout d'un slider pour choisir la distance d'affichage
    slider_dst = Slider(title = 'Distance d\'affichage (km)',
                        start = 0, 
                        end = 100,
                        step = 5, 
                        value = dist,
                        default_size = 250
                        )
    slider_dst.on_change('value', update_dst)

    # Ajout d'un sélecteur pour choisir l'impot à afficher
    select_imp = Select(title="Impôt:", 
                        value="Taxe d'habitation", 
                        options=["Taxe d'habitation", "Taxe foncière"]
                    )
    select_imp.on_change('value', update_impot)

    # Ajout d'un mode daltonien
    checkbox_dalto = CheckboxGroup(labels=["Mode Daltonien"])
    checkbox_dalto.on_change('active', update_colormap)

    # Creation de la choropleth
    choroPlot = create_choropleth(displaySet, defaultParam, defaultPalette, ogCity)
    # Creation de l'historamme
    histoPlot = createHisto(displaySet, defaultParam, defaultPalette, ogCity)
    # Creation des figures infos
    infoTitle, infoDisplaySet = create_info(displaySet, infoParam, ogCity)

    # Organisation colones/lignes
    Col1 = column(slider_yr, slider_dst)
    Col2 = column(select_imp,checkbox_dalto)
    row_wgt = row(Col1, Col2)
    Col3 = column(choroPlot, row_wgt)
    Col4 = column(histoPlot,infoTitle, infoDisplaySet)
    appLayout = row(Col3, Col4)

    doc.add_root(appLayout)
    doc.title = "VizImpôts"

    doc.theme = Theme(filename="theme.yaml")


@app.route('/', methods=['GET'])
def bkapp_page():
    script = server_document('http://localhost:5006/bkapp')
    return render_template("embed.html", script=script, template="Flask")


def bk_worker():
    # Can't pass num_procs > 1 in this configuration. If you need to run multiple
    # processes, see e.g. flask_gunicorn_embed.py
    server = Server({'/bkapp': bkapp}, io_loop=IOLoop(), allow_websocket_origin=["localhost:8000", "localhost:5006"])
    server.start()
    server.io_loop.start()

Thread(target=bk_worker).start()

if __name__ == '__main__':
    print('Opening single process Flask app with embedded Bokeh application on http://localhost:8000/')
    print()
    print('Multiple connections may block the Bokeh app in this configuration!')
    app.run(port=8000)
# %%