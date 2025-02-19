import paramiko
import hashlib
from pathlib import Path
from stat import S_ISDIR

def calculate_local_checksum(file_path, hash_algorithm='sha256'):
    """Oblicza sumę kontrolną lokalnego pliku."""
    hash_func = hashlib.new(hash_algorithm)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def calculate_remote_checksum(sftp, remote_path, hash_algorithm='sha256'):
    """Oblicza sumę kontrolną zdalnego pliku przez SFTP."""
    hash_func = hashlib.new(hash_algorithm)
    with sftp.open(remote_path, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            hash_func.update(data)
    return hash_func.hexdigest()

def get_local_files(base_path):
    """Zwraca słownik lokalnych plików z relatywną ścieżką i sumą kontrolną."""
    files = {}
    for path in Path(base_path).rglob('*'):
        if path.is_file():
            rel_path = path.relative_to(base_path)
            files[str(rel_path)] = {
                'checksum': calculate_local_checksum(path)
            }
    return files

def get_remote_files(sftp, base_path):
    """Rekurencyjnie pobiera pliki zdalne wraz z sumami kontrolnymi."""
    files = {}

    def recursive_list(path, relative_root=Path('.')):
        for entry in sftp.listdir_attr(path):
            full_path = f"{path}/{entry.filename}"
            rel_path = relative_root / entry.filename
            if S_ISDIR(entry.st_mode):
                recursive_list(full_path, rel_path)
            else:
                checksum = calculate_remote_checksum(sftp, full_path)
                files[str(rel_path)] = {
                    'checksum': checksum,
                    'full_path': full_path
                }

    recursive_list(base_path)
    return files

def compare_files(source_files, dest_files):
    """Porównuje pliki na podstawie sum kontrolnych i zwraca różnice."""
    source_set = set(source_files.keys())
    dest_set = set(dest_files.keys())

    added = source_set - dest_set
    deleted = dest_set - source_set
    modified = {
        file for file in source_set & dest_set
        if source_files[file]['checksum'] != dest_files[file]['checksum']
    }

    return added, deleted, modified

def delete_remote_files(sftp, files_to_delete, remote_base_path):
    """Usuwa pliki zdalne, które nie występują w katalogu źródłowym."""
    for file in files_to_delete:
        remote_file_path = f"{remote_base_path}/{file}"
        try:
            sftp.remove(remote_file_path)
            print(f"Usunięto zdalny plik: {remote_file_path}")
        except FileNotFoundError:
            print(f"Plik nie znaleziony do usunięcia: {remote_file_path}")
        except Exception as e:
            print(f"Błąd podczas usuwania {remote_file_path}: {e}")

def compare_directories(source_path, servers, remote_path, username, key_path=None, password=None, auto_delete=False):
    """Porównuje lokalny katalog ze zdalnymi katalogami i usuwa zbędne pliki, jeśli auto_delete=True."""
    source_files = get_local_files(source_path)

    for server in servers:
        print(f"\nPorównanie z serwerem: {server}")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if key_path:
                key = paramiko.RSAKey.from_private_key_file(key_path)
                ssh.connect(server, username=username, pkey=key)
            else:
                ssh.connect(server, username=username, password=password)

            sftp = ssh.open_sftp()
            dest_files = get_remote_files(sftp, remote_path)

            added, deleted, modified = compare_files(source_files, dest_files)

            print("Dodane:", added if added else "Brak")
            print("Skasowane:", deleted if deleted else "Brak")
            print("Zmodyfikowane:", modified if modified else "Brak")

            if auto_delete and deleted:
                confirm = input(f"Czy chcesz usunąć {len(deleted)} zbędnych plików z serwera {server}? (tak/nie): ")
                if confirm.lower() == 'tak':
                    delete_remote_files(sftp, deleted, remote_path)
                else:
                    print("Pominięto usuwanie plików.")

            sftp.close()
        except Exception as e:
            print(f"Błąd podczas połączenia z {server}: {e}")
        finally:
            ssh.close()

# PRZYKŁAD UŻYCIA
servers = ["server1.example.com", "server2.example.com"]
source_directory = "/local/source/path"
remote_directory = "/remote/destination/path"
username = "your_username"
key_path = "/path/to/private/key"  # lub None jeśli używasz hasła
password = None  # lub "your_password" jeśli nie używasz klucza

auto_delete = True  # Ustaw na True, jeśli chcesz automatycznie usuwać zbędne pliki
compare_directories(source_directory, servers, remote_directory, username, key_path, password, auto_delete)
