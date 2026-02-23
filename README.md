# Observatoire de l'emploi territorial

Prototype de data analyst pour scraper, nettoyer, stocker et visualiser des données publiques sur l'emploi en France.

## 🎯 Objectif

Créer un pipeline complet de données pour l'analyse de l'emploi territorial avec un dashboard interactif.

## 📋 Fonctionnalités

- **Scraping** de données d'emploi depuis data.gouv.fr
- **Nettoyage** et structuration des données avec pandas
- **Stockage** dans SQLite avec SQLAlchemy
- **Dashboard** interactif avec Streamlit et Plotly
- **Filtres** par territoire et période
- **Visualisations** temporelles et territoriales
- **Export** des données au format CSV


## 📁 Structure du projet

```
observatoire_emploi/
│
├── data/
│   ├── raw/                 # Données brutes
│   │   └── emploi_raw.csv
│   └── clean/               # Données nettoyées
│       └── emploi_clean.csv
│
├── scripts/
│   ├── scraper.py           # Scraping des données
│   ├── clean_data.py        # Nettoyage des données
│   └── database.py          # Gestion de la base de données
│
├── app/
│   └── dashboard.py         # Dashboard Streamlit
│
├── requirements.txt         # Dépendances Python
├── emploi.db               # Base de données SQLite (créée automatiquement)
└── README.md               # Documentation
```

## 🚀 Installation et lancement

### 1. Cloner le projet

```bash
git clone <https://github.com/oumar1958/datavisualisation-territoriale>
cd observatoire_emploi
```

### 2. Créer un environnement virtuel (recommandé)

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```



### 4. Lancer le dashboard

```bash
streamlit run app/dashboard.py
```

## 📊 Utilisation du dashboard

### Filtres disponibles

- **Type de territoire** : Tous, Département, Région
- **Département** : Sélection spécifique par département
- **Région** : Sélection spécifique par région  
- **Période** : Intervalle d'années avec un slider

### Sections du dashboard

1. **Indicateurs clés** : KPI globaux sur l'emploi et les entreprises
2. **Évolution temporelle** : Graphiques multiples sur l'évolution des indicateurs
3. **Classement** : Top départements selon différentes métriques
4. **Analyse territoriale** : Analyse détaillée par région
5. **Tableau de données** : Tableau interactif avec export CSV

## 📈 Sources de données

Le script tente de récupérer des données réelles depuis :

- **data.gouv.fr** : API publique des datasets français
- **INSEE** : Données statistiques sur l'emploi
- **France Travail** : Données sur le marché du travail

Si aucune donnée réelle n'est disponible, le script génère des données d'exemple réalistes pour tous les départements français (2019-2023).

## 🔧 Configuration

### Modification des sources de données

Pour changer la source de scraping, modifier le fichier `scripts/scraper.py` :

```python
# Modifier l'URL de l'API ou les paramètres de recherche
self.base_url = "https://nouvelle-source-api.com"
```

### Personnalisation du dashboard

Pour ajouter de nouvelles visualisations, modifier `app/dashboard.py` :

```python
# Ajouter une nouvelle section
def render_nouvelle_section(self):
    st.header("Nouvelle section")
    # Votre code ici
```

## 🐛 Dépannage

### Problèmes courants

1. **Erreur de connexion API**
   - Vérifiez votre connexion internet
   - Le script utilisera des données d'exemple si l'API n'est pas accessible

2. **Base de données vide**
   - Assurez-vous d'avoir exécuté les scripts dans l'ordre
   - Vérifiez que les fichiers CSV sont créés dans `data/`

3. **Dashboard ne se lance pas**
   - Vérifiez que Streamlit est installé : `pip install streamlit`
   - Assurez-vous d'être dans le bon répertoire

4. **Erreur de dépendances**
   - Réinstallez les dépendances : `pip install -r requirements.txt`

### Logs et débogage

Les scripts incluent un système de logging détaillé :

```bash
# Pour voir les logs en temps réel
python scripts/scraper.py
```

## 📝 Améliorations possibles

### Features à développer

- [ ] Ajout de véritables cartes géographiques
- [ ] Système de cache pour les API
- [ ] Notifications automatiques de mises à jour
- [ ] Export en PDF des rapports
- [ ] API REST pour l'accès programmatique
- [ ] Tests unitaires automatisés
- [ ] Déploiement sur Cloud (Heroku, AWS)

### Sources de données additionnelles

- [ ] Intégration API Pôle Emploi
- [ ] Données Eurostat
- [ ] Statistiques locales des collectivités
- [ ] Données sur les formations professionnelles

## 📄 Licence

Ce projet est un prototype à but éducatif. Les données utilisées sont publiques et gratuites.

## 🤝 Contribution

Les contributions sont bienvenues ! Pour proposer des améliorations :

1. Fork le projet
2. Créer une branche (`git checkout -b feature/nouvelle-feature`)
3. Commit les changements (`git commit -am 'Ajout nouvelle feature'`)
4. Push vers la branche (`git push origin feature/nouvelle-feature`)
5. Ouvrir une Pull Request

## 📞 Support

Pour toute question ou problème :

- Vérifier la section dépannage ci-dessus
- Consulter les logs des scripts
- Ouvrir une issue sur le repository

---

**Développé avec ❤️ pour un projet de data analyst**
