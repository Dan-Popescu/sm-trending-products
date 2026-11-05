"""Private explicit selected-video effects, without cached source-derived output."""

from __future__ import annotations

import tempfile
from pathlib import Path

import streamlit as st

from .cli import load
from .contracts import ExecutionConfig, VideoRequest, VideoSelection


def render(pipeline, products: list[dict], synthetic: bool) -> None:
    from ..dashboard import display_safe

    service = pipeline.video_research()
    st.subheader("Commentaires, vidéos et transcription sélectionnés")
    st.caption(
        "Aucune collecte supplémentaire au chargement. Téléchargement et transcription "
        "uniquement sur action. TikTok Research demande une approbation propre au projet."
    )
    if not products:
        st.info("Créer un produit et confirmer ses vidéos avant l'enrichissement.")
        return
    product = st.selectbox("Produit à examiner", [row["product_id"] for row in products])
    after = st.text_input("Suite de la liste de vidéos (facultatif)", max_chars=512)
    config_path = st.text_input(
        "Fichier privé de configuration des enrichissements", max_chars=4096
    )
    permission = st.text_input("Référence de permission pour ces vidéos", max_chars=512)
    try:
        listing = service.videos(product, synthetic=synthetic, after=after)
    except Exception:
        st.error("Vidéos indisponibles : vérifier la propriété et les sources.")
        return
    videos = listing["videos"]
    if listing["next_after"]:
        st.caption("Suite de la liste : " + listing["next_after"])
    if not videos:
        st.info("Aucune vidéo confirmée dans cette page.")
        return
    selected = st.selectbox(
        "Vidéo",
        range(len(videos)),
        format_func=lambda index: videos[index]["selection"]["entity_key"],
    )
    item = videos[selected]
    st.write(item["title"])
    st.caption("Le texte des commentaires n'établit ni un achat ni un acheteur indépendant.")
    video_url = st.text_input(
        "Lien canonique de la vidéo TikTok", value=item["video_url"] or "", max_chars=2048
    )
    request_id = st.text_input("Identifiant stable de la demande", max_chars=128)
    snapshot = st.text_input(
        "Identifiant de relevé des commentaires", value="initial", max_chars=128
    )
    provider = st.selectbox("Accès aux commentaires", ["unofficial", "research"])
    media_job = st.text_input(
        "Identifiant du téléchargement pour transcrire ou exporter", max_chars=64
    )
    operation = None
    if st.button("Récupérer les commentaires sélectionnés"):
        operation = "comments"
    if st.button("Télécharger cette vidéo"):
        operation = "download"
    if st.button("Transcrire avec Whisper Large-v3 local"):
        operation = "transcribe"
    read = st.button("Lire le résultat enregistré")
    import_text = st.button("Importer la transcription dans les analyses de texte")
    export = st.button("Préparer le fichier vidéo à enregistrer")
    if not any((operation, read, import_text, export)):
        return
    try:
        config = ExecutionConfig.model_validate(load(Path(config_path)))
        selection = VideoSelection.model_validate(
            {**item["selection"], "permission_reference": permission}
        )
        if operation:
            request = VideoRequest.model_validate(
                {
                    "request_id": request_id,
                    "selection": selection,
                    "operation": operation,
                    "provider": provider if operation == "comments" else "unofficial",
                    "video_url": video_url or None,
                    "refresh_key": snapshot,
                    "media_job_id": media_job if operation == "transcribe" else None,
                }
            )
            output = service.run(request, config)
        elif import_text:
            output = service.import_transcription(request_id, product, config, synthetic=synthetic)
        elif export:
            # Bound Streamlit's browser-download buffer independently of CLI streaming.
            if config.max_media_bytes > 100_000_000:
                st.error(
                    "L'export navigateur est limité à 100 Mo ; utiliser l'export fichier "
                    "pour un plafond supérieur."
                )
                return
            with tempfile.TemporaryDirectory(prefix="selected-video-export-") as directory:
                target = Path(directory) / "video.mp4"
                output = service.export_media(media_job, selection, config, target)
                st.download_button(
                    "Enregistrer la vidéo",
                    data=target.read_bytes(),
                    file_name="selected-video.mp4",
                    mime="video/mp4",
                )
        else:
            output = service.result(request_id, product, config, synthetic=synthetic)
        st.json(display_safe(output))
    except Exception:
        st.error(
            "Action refusée ou incomplète. Vérifier la configuration, les permissions, "
            "les dépendances, le modèle local et les limites. Une demande incertaine "
            "n'est pas relancée automatiquement."
        )
