Parfait. Voici un **premier jet complet du “Dataset Design Document”** pour ton RAG.
Je l’ai rédigé comme le ferait un document de cadrage professionnel pour une équipe data/IA : clair, structuré, et exploitable immédiatement pour définir les jeux de données, les tâches, et les métriques.

---

# 🧭 **Dataset Design Document – RAG “Gestion Territoriale et Environnementale”**

---

## 1. 🎯 Objectif général

Concevoir un **jeu de données RAG réaliste et compact** permettant d’entraîner et d’évaluer des modèles capables de :

* **retrouver**, **croiser** et **synthétiser** des informations contenues dans des rapports techniques environnementaux non OCRisés ;
* **raisonner géographiquement** à partir d’informations textuelles et visuelles ;
* **répondre à des requêtes métiers** formulées par des acteurs de la gestion territoriale.

Le domaine choisi est la **gestion d’un bassin fluvial** (ex : Loire, Garonne, Seine, Meuse), intégrant des rapports d’études, de suivi environnemental, et d’aménagements locaux.

---

## 2. 🧱 Contexte métier

Les agences de l’eau, bureaux d’études, collectivités et urbanistes produisent chaque année des centaines de rapports (3 à 10 pages) sur :

* la **qualité de l’eau**,
* les **pollutions diffuses ou ponctuelles**,
* les **risques naturels** (inondations, sécheresses),
* les **aménagements** (barrages, stations, zones humides),
* la **biodiversité et l’occupation des sols**.

Ces rapports sont souvent :

* en **PDF non OCRisés**,
* avec des **cartes, schémas et graphiques non textuels**,
* sans structure unifiée, ni métadonnées normalisées.

Un RAG sur ce corpus doit permettre à un utilisateur de **poser une question métier** et d’obtenir une réponse contextualisée, sourcée et synthétique.

---

## 3. 👥 Utilisateurs cibles

| Rôle                  | Objectif                                                                     | Type de requêtes                |
| --------------------- | ---------------------------------------------------------------------------- | ------------------------------- |
| Ingénieur hydrologue  | Identifier anomalies, zones à risque, tendances                              | Techniques et spatiales         |
| Urbaniste territorial | Vérifier la compatibilité d’un projet avec les contraintes environnementales | Géographiques et réglementaires |
| Collectivité locale   | Comprendre et communiquer sur la qualité de l’eau et les actions             | Synthétiques                    |
| Bureau d’étude        | Consolider plusieurs rapports pour une analyse comparative                   | Corrélées et temporelles        |

---

## 4. 📦 Composition du dataset

| Élément                         | Détail                                                                                                                                                                                                         | Volume cible                                  |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Corpus documentaire**         | Rapports PDF scannés (non OCR), 3–10 pages chacun, en français                                                                                                                                                 | ~200–300 documents                            |
| **Types de contenu**            | Texte brut, tableaux, cartes géographiques, photographies, graphiques                                                                                                                                          | 70 % texte, 30 % visuel                       |
| **Métadonnées structurées**     | - Titre du rapport  <br> - Année  <br> - Type d’étude (qualité, aménagement, biodiversité…)  <br> - Zone géographique (commune, bassin)  <br> - Source / auteur                                                | Format JSON ou CSV                            |
| **Annotations supplémentaires** | - Entités géographiques (commune, département, rivière, station, etc.)  <br> - Valeurs numériques clés (nitrates, phosphates, etc.)  <br> - Lien image ↔ texte  <br> - Table de correspondance lieux–documents | Semi-automatique (via regex + tagging manuel) |

---

## 5. 🔎 Types de requêtes représentées

Les requêtes (≈ 300) sont réparties selon 5 familles pour couvrir la diversité des usages RAG :

| Famille                     | Exemple de requête                                                                    | Compétence testée         |
| --------------------------- | ------------------------------------------------------------------------------------- | ------------------------- |
| **Factuelles locales**      | « Quelle est la concentration en nitrates à Chalon-sur-Loire en 2023 ? »              | Extraction directe        |
| **Spatiales corrélées**     | « Quelles communes en amont du barrage de Saint-Roch présentent un taux similaire ? » | Raisonnement géographique |
| **Temporelles / évolution** | « Comment a évolué la qualité de l’eau entre 2015 et 2022 ? »                         | Fusion multi-documents    |
| **Thématiques croisées**    | « Y a-t-il un lien entre pollution agricole et turbidité dans la zone aval ? »        | Corrélation conceptuelle  |
| **Synthèse / décision**     | « Résume la situation hydrologique du bassin de la Sarthe en 2022. »                  | Génération narrative      |

---

## 6. 🧠 Structure de chaque entrée (exemple)

```json
{
  "document_id": "rapport_042",
  "title": "Suivi de la qualité de l’eau sur le bassin de la Sarthe – 2023",
  "year": 2023,
  "zone": "Bassin Sarthe moyenne",
  "type": "Qualité de l’eau",
  "text_content": "...",
  "figures": [
      {"id": "fig1", "type": "carte", "caption": "Zones de prélèvement", "ocr_text": null},
      {"id": "fig2", "type": "graphique", "caption": "Évolution nitrates", "ocr_text": null}
  ],
  "metadata": {
    "source": "Agence de l’eau Loire-Bretagne",
    "pages": 8
  },
  "qa_pairs": [
    {
      "query": "Quelle est la qualité de l’eau sur la Sarthe moyenne en 2023 ?",
      "answer_reference": "Extrait p.3 : 'Les analyses montrent une qualité moyenne avec une teneur en nitrates de 45 mg/L.'",
      "answer_summary": "Qualité moyenne, 45 mg/L de nitrates",
      "query_type": "factuelle"
    }
  ]
}
```

---

## 7. 🗺️ Dimensions clés à annoter

| Dimension              | Méthode d’annotation                                    | Exemple                         |
| ---------------------- | ------------------------------------------------------- | ------------------------------- |
| **Géographie**         | Nom de lieu + coordonnées GPS (si carte)                | “Saint-Avertin” → (47.37, 0.73) |
| **Thématique**         | Tag manuel ou semi-auto (pollution, biodiversité, etc.) | “pollution_eau”, “hydrologie”   |
| **Valeurs numériques** | Extraction regex (mg/L, °C, m³/s, etc.)                 | “Nitrates : 45 mg/L”            |
| **Type d’image**       | Heuristique de détection (carte/photo/graphique)        | “carte raster couleur”          |
| **Date / période**     | Parsing automatique                                     | “Campagne de 2018–2022”         |

---

## 8. 🧩 Évaluation du RAG

| Critère              | Métrique                                                            | Description                            |
| -------------------- | ------------------------------------------------------------------- | -------------------------------------- |
| **Retrieval**        | Recall@k, MRR                                                       | Capacité à retrouver les bons passages |
| **Answer relevance** | ROUGE / BERTScore                                                   | Qualité textuelle de la réponse        |
| **Geo-cohérence**    | Score d’accord géographique (distance entre zone réelle et réponse) | Spécifique à ce dataset                |
| **Factualité**       | Human eval ou GPT-based factuality                                  | Vérifie absence d’hallucinations       |
| **Multimodalité**    | Score de précision image–texte (pour les figures)                   | Si image encodée                       |

---

## 9. ⚙️ Pipeline de création

| Étape                      | Outil / Méthode                                                    | Résultat          |
| -------------------------- | ------------------------------------------------------------------ | ----------------- |
| 1️⃣ Collecte               | Téléchargement de rapports publics (agences de l’eau, DREAL, etc.) | Corpus brut       |
| 2️⃣ Préparation            | Découpage PDF, extraction d’images, nettoyage métadonnées          | Corpus structuré  |
| 3️⃣ Annotation             | Semi-automatique (spacy, regex, GPT-labeling)                      | Données enrichies |
| 4️⃣ Génération de requêtes | À partir des titres, tableaux et entités détectées                 | 300 requêtes Q/A  |
| 5️⃣ Validation humaine     | Vérification d’un échantillon (5 %)                                | Dataset validé    |

---

## 10. 🧩 Pistes d’évolution

* Extension à d’autres bassins (comparaison interrégionale)
* OCR adaptatif (Vision-LLM pour inférence d’image)
* Intégration de couches SIG (shapefiles, GeoJSON)
* Création de “scénarios de requêtes utilisateurs” interactifs (chat simulé)
* Benchmark ouvert pour RAG géospatial francophone

---

## 11. 📄 Format final

* Dossier `/documents/` → PDF originaux
* Dossier `/images/` → figures extraites
* Fichier `/metadata.jsonl` → structure documentaire
* Fichier `/queries.jsonl` → requêtes et réponses de référence
* Fichier `/splits.json` → séparation train/dev/test

---

## 12. 🧭 Objectif final du RAG

Créer un système capable de répondre à des requêtes comme :

> “Liste les communes en amont du barrage de Saint-Roch ayant connu une dégradation de la qualité de l’eau entre 2018 et 2023, et résume les principales causes identifiées.”

Ce type de requête nécessite :

* la compréhension géographique (amont/aval),
* la fusion temporelle (2018–2023),
* la recherche multimodale (texte + carte),
* la synthèse fiable.

---

Souhaites-tu que je t’en fasse maintenant la **version “opérationnelle”** (c’est-à-dire le plan concret de construction du dataset : arborescence, scripts, types de fichiers, outils OCR/embedding recommandés, et stratégie de génération de requêtes) ?
→ Ce serait la *partie 2 : Implementation Plan* du même document.
