# Importación de bibliotecas estándar y de terceros
import os
import sys
import gzip
import bz2
import zipfile
import tarfile
import tempfile
import time
import shutil
from pathlib import Path
from threading import Thread
from tkinter import (
    Tk, Button, Label, Listbox, Scrollbar, END,
    filedialog, messagebox, Text, VERTICAL, RIGHT, Y, LEFT, BOTH, Frame, Checkbutton, IntVar, Entry, StringVar, simpledialog
)
from dask import delayed, compute  # Para paralelismo con Dask
from Crypto.Cipher import AES  # Encriptación AES
from Crypto.Random import get_random_bytes  # Generación segura de claves aleatorias
from pydrive2.auth import GoogleAuth  # Autenticación Google
from pydrive2.drive import GoogleDrive  # Subida a Google Drive

# Clase principal del sistema de backup
class SecureBackupSystem:
    def __init__(self, master):
        self.master = master
        master.title("Sistema de Backup Seguro")
        master.geometry("780x720")
        self.selected_dirs = []  # Lista de carpetas seleccionadas para respaldo
        self.external_path = None  # Ruta del disco externo
        self.drive = None  # Objeto de Google Drive

        self.init_gui()  # Inicializa la interfaz
        self.authenticate_google_drive()  # Autenticación Google Drive

    # Método para construir la interfaz gráfica (GUI)
    def init_gui(self):
        top = Frame(self.master)
        top.pack(pady=5)

        format_frame = Frame(self.master)
        format_frame.pack(pady=5)

        mid = Frame(self.master)
        mid.pack(pady=5, fill=BOTH, expand=True)

        dest = Frame(self.master)
        dest.pack(pady=5)

        bottom = Frame(self.master)
        bottom.pack(pady=5, fill=BOTH, expand=True)

        # Botones para agregar o eliminar carpetas
        self.btn_add = Button(top, text="Agregar Carpeta", command=self.add_folder)
        self.btn_add.pack(side=LEFT, padx=5)

        self.btn_remove = Button(top, text="Eliminar Carpeta", command=self.remove_folder)
        self.btn_remove.pack(side=LEFT, padx=5)

        # Selección de tipo de compresión
        Label(format_frame, text="Compresión:").pack(side=LEFT)
        self.use_gzip = IntVar(value=1)
        self.use_bzip2 = IntVar()
        self.use_zip = IntVar()
        Checkbutton(format_frame, text="GZIP", variable=self.use_gzip).pack(side=LEFT)
        Checkbutton(format_frame, text="BZIP2", variable=self.use_bzip2).pack(side=LEFT)
        Checkbutton(format_frame, text="ZIP", variable=self.use_zip).pack(side=LEFT)

        # Lista de carpetas agregadas
        self.dir_list = Listbox(mid)
        self.dir_list.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar = Scrollbar(mid, orient=VERTICAL, command=self.dir_list.yview)
        scrollbar.pack(side=RIGHT, fill=Y)
        self.dir_list.config(yscrollcommand=scrollbar.set)

        # Opciones de almacenamiento
        Label(dest, text="Destino:").grid(row=0, column=0, sticky='w')
        self.to_local = IntVar(value=1)
        self.to_external = IntVar()
        self.to_usb = IntVar()
        self.to_cloud = IntVar()
        Checkbutton(dest, text="Local", variable=self.to_local).grid(row=1, column=0, sticky='w')
        Checkbutton(dest, text="Disco Externo", variable=self.to_external).grid(row=2, column=0, sticky='w')
        Checkbutton(dest, text="USB Fragmentado", variable=self.to_usb).grid(row=3, column=0, sticky='w')
        Checkbutton(dest, text="Google Drive", variable=self.to_cloud).grid(row=4, column=0, sticky='w')

        # Entrada para tamaño de fragmento en USB
        Label(dest, text="Tamaño fragmento USB (MB):").grid(row=3, column=1, sticky='e')
        self.usb_size = StringVar(value="100")
        Entry(dest, textvariable=self.usb_size, width=5).grid(row=3, column=2)
        Button(dest, text="Seleccionar Disco Externo", command=self.select_external).grid(row=2, column=1, padx=5)
        self.external_var = StringVar(value="")
        Label(dest, textvariable=self.external_var, width=40).grid(row=2, column=2, sticky='w')

        # Área de registro de eventos (log)
        self.log = Text(bottom, height=15, state='disabled')
        self.log.pack(fill=BOTH, expand=True)

        # Botones de acciones principales
        Button(self.master, text="Iniciar Backup", command=self.start_backup_thread).pack(pady=5)
        Button(self.master, text="Restaurar Backup", command=self.restore_backup).pack(pady=5)

    # Función auxiliar para registrar mensajes en la GUI
    def log_msg(self, msg):
        self.log.config(state='normal')
        self.log.insert(END, msg + '\n')
        self.log.see(END)
        self.log.config(state='disabled')

    # Agrega carpeta a respaldar
    def add_folder(self):
        path = filedialog.askdirectory()
        if path and path not in self.selected_dirs:
            self.selected_dirs.append(path)
            self.dir_list.insert(END, path)
            self.log_msg(f"Agregada: {path}")

    # Elimina carpeta seleccionada
    def remove_folder(self):
        idxs = list(self.dir_list.curselection())
        for i in reversed(idxs):
            path = self.dir_list.get(i)
            self.dir_list.delete(i)
            self.selected_dirs.remove(path)
            self.log_msg(f"Eliminada: {path}")

    # Selección de disco externo
    def select_external(self):
        path = filedialog.askdirectory()
        if path:
            self.external_path = path
            self.external_var.set(path)
            self.log_msg(f"Disco externo: {path}")

    # Autenticación con Google Drive
    def authenticate_google_drive(self):
        try:
            auth = GoogleAuth()
            auth.LocalWebserverAuth()
            self.drive = GoogleDrive(auth)
            self.log_msg("Google Drive autenticado.")
        except Exception as e:
            self.log_msg(f"Error autenticación: {e}")

    # Inicia backup en un hilo independiente
    def start_backup_thread(self):
        t = Thread(target=self.perform_backup)
        t.start()

    # Proceso principal de backup
    def perform_backup(self):
        if not self.selected_dirs:
            messagebox.showwarning("Error", "No hay carpetas seleccionadas.")
            return
        if not (self.use_gzip.get() or self.use_bzip2.get() or self.use_zip.get()):
            messagebox.showwarning("Error", "Seleccione al menos un formato de compresión.")
            return
        if not (self.to_local.get() or self.to_external.get() or self.to_usb.get() or self.to_cloud.get()):
            messagebox.showwarning("Error", "Seleccione al menos un destino.")
            return

        temp_tar = filedialog.asksaveasfilename(defaultextension=".tar", filetypes=[("TAR", "*.tar")])
        if not temp_tar:
            self.log_msg("Operación cancelada.")
            return

        try:
            with tempfile.TemporaryDirectory() as tmp:
                tasks = []
                for folder in self.selected_dirs:
                    for root, _, files in os.walk(folder):
                        for file in files:
                            src = os.path.join(root, file)
                            rel = os.path.relpath(src, folder)
                            if self.use_gzip.get():
                                dest = os.path.join(tmp, "gz", rel + ".gz")
                                tasks.append(delayed(self.compress_gzip)(src, dest))
                            if self.use_bzip2.get():
                                dest = os.path.join(tmp, "bz2", rel + ".bz2")
                                tasks.append(delayed(self.compress_bzip2)(src, dest))
                    if self.use_zip.get():
                        dest = os.path.join(tmp, "zip", Path(folder).name + ".zip")
                        tasks.append(delayed(self.compress_zip)(folder, dest))

                compute(*tasks)
                with tarfile.open(temp_tar, "w") as tar:
                    for base in ["gz", "bz2", "zip"]:
                        base_dir = os.path.join(tmp, base)
                        if os.path.exists(base_dir):
                            for root, _, files in os.walk(base_dir):
                                for f in files:
                                    full = os.path.join(root, f)
                                    arc = os.path.relpath(full, tmp)
                                    tar.add(full, arcname=arc)

                encrypted_path = temp_tar + ".aes"
                self.encrypt_file(temp_tar, encrypted_path)
                self.log_msg(f"Backup encriptado: {encrypted_path}")

                self.store_backup(encrypted_path, temp_tar)
        except Exception as e:
            self.log_msg(f"Error en backup: {e}")

    # Compresión con GZIP
    def compress_gzip(self, src, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(src, 'rb') as fin, gzip.open(dest, 'wb') as fout:
            shutil.copyfileobj(fin, fout)

    # Compresión con BZIP2
    def compress_bzip2(self, src, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(src, 'rb') as fin, bz2.open(dest, 'wb') as fout:
            shutil.copyfileobj(fin, fout)

    # Compresión ZIP
    def compress_zip(self, folder, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with zipfile.ZipFile(dest, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder):
                for f in files:
                    full = os.path.join(root, f)
                    arc = os.path.relpath(full, folder)
                    zipf.write(full, arcname=arc)

    # Encripta archivo TAR generado
    def encrypt_file(self, inp, out):
        key = get_random_bytes(32)
        cipher = AES.new(key, AES.MODE_GCM)
        with open(inp, 'rb') as f:
            data = f.read()
        ct, tag = cipher.encrypt_and_digest(data)
        with open(out, 'wb') as f:
            f.write(cipher.nonce + tag + ct)
        self.log_msg(f"Clave AES (hex): {key.hex()}")

    # Almacenamiento en los distintos destinos
    def store_backup(self, encrypted_path, raw_tar):
        if self.to_local.get():
            local_dir = os.path.join(os.path.expanduser("~"), "secure_backups")
            os.makedirs(local_dir, exist_ok=True)
            shutil.copy2(encrypted_path, local_dir)
            self.log_msg(f"Backup guardado local: {local_dir}")

        if self.to_external.get() and self.external_path:
            shutil.copy2(encrypted_path, self.external_path)
            self.log_msg(f"Backup guardado externo: {self.external_path}")

        if self.to_usb.get():
            size_mb = int(self.usb_size.get())
            chunk = size_mb * 1024 * 1024
            base = os.path.basename(encrypted_path)
            usb_path = filedialog.askdirectory(title="Carpeta USB destino")
            with open(encrypted_path, 'rb') as f:
                i = 0
                while True:
                    part = f.read(chunk)
                    if not part:
                        break
                    with open(os.path.join(usb_path, f"{base}.part{i:03}"), 'wb') as frag:
                        frag.write(part)
                    i += 1
                    self.log_msg(f"Fragmento {i} guardado")

        if self.to_cloud.get() and self.drive:
            for fpath in [encrypted_path, raw_tar]:
                file_drive = self.drive.CreateFile({'title': os.path.basename(fpath)})
                file_drive.SetContentFile(fpath)
                file_drive.Upload()
                self.log_msg(f"Subido a Drive: {fpath}")

    # Restaurar backup desde archivo encriptado
    def restore_backup(self):
        enc_file = filedialog.askopenfilename(title="Seleccionar archivo .aes")
        if not enc_file:
            return
        key_hex = simpledialog.askstring("Clave", "Introduce la clave AES (hex):")
        if not key_hex:
            return
        try:
            key = bytes.fromhex(key_hex)
            with open(enc_file, 'rb') as f:
                nonce = f.read(16)
                tag = f.read(16)
                data = f.read()
            cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
            plain = cipher.decrypt_and_verify(data, tag)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".tar") as tmp_tar:
                tmp_tar.write(plain)
                tar_path = tmp_tar.name
            dest = filedialog.askdirectory(title="Carpeta de restauración")
            with tarfile.open(tar_path, 'r') as tar:
                tar.extractall(dest)
            self.log_msg(f"Backup restaurado en: {dest}")
        except Exception as e:
            self.log_msg(f"Error al restaurar: {e}")

# Punto de entrada principal
if __name__ == '__main__':
    root = Tk()
    app = SecureBackupSystem(root)
    root.mainloop()
