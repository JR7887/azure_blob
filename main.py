import sys
import argparse
import configparser
import logging
import os.path
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

"""BlobServiceClient allows us to manipulate the resources of AZURE stockage et the blob containers.
ConainerClient allows us to manipulate the containers of the AZURE stockage and theirs blobs.
BlobClient , this class allows us to manipulate the blobs of the AZURE stockage.
"""


def listb(args, containerclient):

    """This function list the blobs in the container"""
    blob_list=containerclient.list_blobs()
    for blob in blob_list:
        print(blob.name)


def upload(cible, blobclient):

    """This function allows us  to upload a blob from our PC to the  conteneur AZURE"""
    logging.info(f"Ouverture du fichier {cible} pour envoyer ")
    with open(cible, "rb") as f:
        logging.warning(f"envoi les fichiers sur le container {blobclient}")
        blobclient.upload_blob(f)


def download(filename, dl_folder, blobclient):

    """This function allows us to download a blob from the conteneur AZURE to our PC"""
    logging.info(f"Ouverture du fichier {filename} pour le telecharger ")
    with open(os.path.join(dl_folder,filename), "wb") as my_blob:
        logging.warning(f"recuperation des fichiers {filename} sur le container {blobclient}")
        blob_data=blobclient.download_blob()
        blob_data.readinto(my_blob)


def main(args,config):

    """This function get the properties  from the ContainerClient to interact with a 
    specific blob if the argument is list , it will return the blobs in the container,
    else if the argument is upload it will upload the blob from the contenur AZURE to our pc 
    otherwise if the argument is download it will download the blob from the conteneur to our pc
    """
    logging.info("lancement de la fonction main")
    blobclient=BlobServiceClient(
        f"https://{config['storage']['account']}.blob.core.windows.net",
        config["storage"]["key"],
        logging_enable=False)
    logging.debug("connection au compte de stockage effectuer")
    containerclient=blobclient.get_container_client(config["storage"]["container"])
    logging.debug("connection au container de stockage")
    if args.action=="list":
        logging.debug("l'arg list a été passé. Lancement de la fonction liste")
        return listb(args, containerclient)
    else:
        if args.action=="upload":
            blobclient=containerclient.get_blob_client(os.path.basename(args.cible))
            logging.debug("arg upload a été passé. Lancement de la fonction upload")
            return upload(args.cible, blobclient)
        elif args.action=="download":
            logging.debug("arg download a été passé. Lancement de la fonction download")
            blobclient=containerclient.get_blob_client(os.path.basename(args.remote))
            return download(args.remote, config["general"]["restoredir"], blobclient)
    

if __name__=="__main__":
    parser=argparse.ArgumentParser("Logiciel d'archivage de documents")
    parser.add_argument("-cfg",default="config.ini",help="chemin du fichier de configuration")
    parser.add_argument("-lvl",default="info",help="niveau de log")
    subparsers=parser.add_subparsers(dest="action",help="type d'operation")
    subparsers.required=True
    
    parser_s=subparsers.add_parser("upload")
    parser_s.add_argument("cible",help="fichier à envoyer")

    parser_r=subparsers.add_parser("download")
    parser_r.add_argument("remote",help="nom du fichier à télécharger")
    parser_r=subparsers.add_parser("list")

    args=parser.parse_args()

    #erreur dans logging.warning : on a la fonction au lieu de l'entier
    loglevels={"debug":logging.DEBUG, "info":logging.INFO, "warning":logging.WARNING, "error":logging.ERROR, "critical":logging.CRITICAL}
    print(loglevels[args.lvl.lower()])
    logging.basicConfig(level=loglevels[args.lvl.lower()])

    config=configparser.ConfigParser()
    config.read(args.cfg)

    sys.exit(main(args,config))
