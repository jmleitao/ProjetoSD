# Googol - Meta 1

## Instalação

    python -m pip install -r requirements.txt
    python gen_proto.py      

## Arranque (um terminal por linha, na raiz do projeto)

    python gateway.py
    python barrel.py --port 8184 --name barrel1
    python barrel.py --port 8185 --name barrel2 --peer 127.0.0.1:8184
    python -m googol.downloader --id d1 --seed https://www.uc.pt
    python -m googol.downloader --id d2                   # mais Downloaders em paralelo
    python client.py

Cada Barrel guarda os dados em `<nome>.db` (SQLite). Para recomeçar do zero, apagar
`*.db*`, `outbox_*.jsonl` e `gateway_state.json` com tudo desligado.

## Testes da lógica (sem gRPC nem rede)

    python -m unittest discover -s tests -t . -v

## Testar em 2 máquinas

Em ambas as máquinas ter os requirements e os codigos gerados
    python -m pip install -r requirements.txt
    python gen_proto.py

Descobrir o endereço IPv4 das máquinas
    ipconfig

Alterar config.properties. Escolher uma das máquinas como host para o gateway. Exemplo: máquina A: 192.168.1.00    máquina B: 192.168.2.00
    (no config.properties)
    gateway_host = 192.168.1.00
    barrels = 192.168.1.80:8184,192.168.2.00:8185

Na máquina A (host):
    python gateway.py
    python barrel.py --port 8184 --name barrel1 --host 192.168.1.00
    python -m googol.downloader --id d1 --seed <https://www.uc.pt>

Na máquina B:
    python barrel.py --port 8185 --name barrel2 --host 192.168.2.00
    python -m googol.downloader --id d2
    python client.py

## Possiveis problemas

Verificar se as maquinas conseguem comunicar
    Test-NetConnection 192.168.1.00 -Port 8183      (na máquina B)

Se nao conseguirem, desativar Firewall:
    New-NetFirewallRule -DisplayName "Googol" -Direction Inbound -Protocol TCP -LocalPort 8183-8185 -Action Allow
