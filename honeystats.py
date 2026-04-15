
import os
import time
import json
import threading
from web3 import Web3
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

# --- Configuration ---

PUSHGATEWAY_ADDRESS = os.environ.get("PUSHGATEWAY_ADDRESS", "https://pgw.godfather2.ethswarm.org")
DATA_DIR = "./data"
LAST_BLOCK_FILE = os.path.join(DATA_DIR, "last_block.json")
LAST_BLOCK_FILE_REDISTRIBUTION = os.path.join(DATA_DIR, "last_block_redistribution.json")
LAST_BLOCK_FILE_STAKING = os.path.join(DATA_DIR, "last_block_staking.json")
STAKERS_FILE = os.path.join(DATA_DIR, "stakers.json")
WINNERS_FILE = os.path.join(DATA_DIR, "winners.json")

# BZZ Token has 16 decimal places, not 18 like ETH
BZZ_DECIMALS = 16

CHAINS = {
    "sepolia": {
        "name": "Sepolia Testnet",
        "rpc_url": os.environ.get("SEPOLIA_RPC_URL", "https://sep.swarm1.ethswarm.org"),
        "contracts": {
            "bzztoken": {
                "name": "BzzToken",
                "address": "0x543dDb01Ba47acB11de34891cD86B675F04840db",
                "deployment_block": 4594507,
            },
            "redistribution": {
                "name": "Redistribution",
                "address": "0x5b718E36F5Ce2F2F7e25A397040436Ce6af3e89e",
                "deployment_block": 8646721,
            },
            "postagestamp": {
                "name": "PostageStamp",
                "address": "0xcdfdC3752caaA826fE62531E0000C40546eC56A6",
                "deployment_block": 6596277,
            },
            "priceoracle": {
                "name": "PriceOracle",
                "address": "0x95Dc18380e92C13E4F8a4e94C99FB1b97250174B",
                "deployment_block": 8226873,
            },
            "staking": {
                "name": "Staking",
                "address": "0xEEF13Ef9eD9cDD169701eeF3cd832df298dD1bB4",
                "deployment_block": 8262529,
            },
        },
    },
    "gnosis": {
        "name": "Gnosis Chain",
        "rpc_url": os.environ.get("GNOSIS_RPC_URL", "https://gno.prod.ethswarm.org"),
        "contracts": {
            "bzztoken": {
                "name": "BzzToken",
                "address": "0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da",
                "deployment_block": 16514506,
            },
            "redistribution": {
                "name": "Redistribution",
                "address": "0x5069cdfB3D9E56d23B1cAeE83CE6109A7E4fd62d",
                "deployment_block": 41105199,
                "versions": [
                    {"address": "0x8c26b7CA61A6608B011cBa43d8cA4476B6D8dA17", "from_block": 25527081, "to_block": 27391083},
                    {"address": "0x1F9a1FDe5c6350E949C5E4aa163B4c97011199B4", "from_block": 27391083, "to_block": 31305674},
                    {"address": "0x0964c834C660C44E0afd3B7F10F19f275ee31411", "from_block": 31305674, "to_block": 34159666},
                    {"address": "0xD9dFE7b0ddc7CcA41304FE9507ed823faD3bdBab", "from_block": 34159666, "to_block": 35961755},
                    {"address": "0xFfF73fd14537277B3F3807e1AB0F85E17c0ABea5", "from_block": 35961755, "to_block": 37339181},
                    {"address": "0x69C62CaCd68C2CBBf3D0C7502eF556DB3AC7889B", "from_block": 37339181, "to_block": 40430243},
                    {"address": "0x9f9A8dA5A0Db2611f9802ba1a0B99cC4A1c3b6A2", "from_block": 40430243, "to_block": 41105199},
                    {"address": "0x5069cdfB3D9E56d23B1cAeE83CE6109A7E4fd62d", "from_block": 41105199, "to_block": None}
                ]
            },
            "priceoracle": {
                "name": "PriceOracle",
                "address": "0x47EeF336e7fE5bED98499A4696bce8f28c1B0a8b",
                "deployment_block": 37339168,
            },
            "postagestamp": {
                "name": "PostageStamp",
                "address": "0x45a1502382541Cd610CC9068e88727426b696293",
                "deployment_block": 31305656,
                "versions": [
                    {"address": "0x6a1A21ECA3aB28BE85C7Ba22b2d6eAE5907c900E", "from_block": 16515648, "to_block": 25527076},
                    {"address": "0x30d155478eF27Ab32A1D578BE7b84BC5988aF381", "from_block": 25527076, "to_block": 31305656},
                    {"address": "0x45a1502382541Cd610CC9068e88727426b696293", "from_block": 31305656, "to_block": None}
                ]
            },
            "staking": {
                "name": "Staking",
                "address": "0xda2a16EE889E7F04980A8d597b48c8D51B9518F4",
                "deployment_block": 40430237,
                "versions": [
                    {"address": "0x445B848e16730988F871c4a09aB74526d27c2Ce8", "from_block": 37339175, "to_block": 40430237},
                    {"address": "0xda2a16EE889E7F04980A8d597b48c8D51B9518F4", "from_block": 40430237, "to_block": None}
                ]
            },
        },
    },
}


def get_abi(contract_key, chain_name):
    """Loads the ABI of a contract from a local file."""
    abi_path = os.path.join("abi", f"{contract_key}_{chain_name}.json")
    with open(abi_path, 'r') as f:
        abi_string = f.read()
    return json.loads(abi_string)


def get_current_outpayment(price_updates, target_block):
    """
    Calculates currentTotalOutpayment at a target block based on price update history.
    price_updates: list of {"block": int, "price": int}
    """
    if not price_updates:
        return 0
    
    # Sort updates by block
    sorted_updates = sorted(price_updates, key=lambda x: x["block"])
    
    total_outpayment = 0
    prev_block = sorted_updates[0]["block"]
    prev_price = 0 # Price before the first update is 0
    
    for update in sorted_updates:
        if update["block"] > target_block:
            break
        
        # Add outpayment accumulated since last update
        total_outpayment += (update["block"] - prev_block) * prev_price
        
        prev_block = update["block"]
        prev_price = update["price"]
        
    # Add outpayment since the last update before target_block
    if target_block > prev_block:
        total_outpayment += (target_block - prev_block) * prev_price
        
    return total_outpayment


def is_simple_output(outputs):
    """Checks if the function's output is a simple type suitable for a Prometheus Gauge."""
    if not outputs:
        return False
    if len(outputs) > 1:
        return False
    
    output_type = outputs[0].get('type', '')
    return output_type.startswith('uint') or output_type.startswith('int') or output_type == 'bool'


print_lock = threading.Lock()

def process_winner_events(registry, error_counter):
    """Processes winner events from the Gnosis chain and creates a leaderboard."""
    chain_name = "gnosis"
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_key = "redistribution"
    contract_config = chain_config["contracts"][contract_key]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    contract_address = contract_config["address"]

    with print_lock:
        print(f"Processing winner events for {contract_friendly_name} on {chain_config['name']}...")

    # Load last processed block number
    current_block = w3.eth.block_number
    last_block_data = {}
    try:
        with open(LAST_BLOCK_FILE, 'r') as f:
            last_block_data = json.load(f)
            last_block = last_block_data.get(chain_name, contract_config.get("deployment_block", 0))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load last block file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_last_block').inc()
        last_block = contract_config.get("deployment_block", 0)
        last_block_data[chain_name] = last_block
        with open(LAST_BLOCK_FILE, 'w') as f:
            json.dump(last_block_data, f)

    # Load winners data
    winners_file_path = os.path.join(DATA_DIR, f"winners_{chain_name}.json")
    try:
        with open(winners_file_path, 'r') as f:
            winners_data = json.load(f)
            if isinstance(winners_data, dict):
                # Convert old format to new format
                winners = []
                for owner, stake in winners_data.items():
                    winners.append({"owner": owner, "stake": stake, "timestamp": 0})
            else:
                winners = winners_data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load winners file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_winners_file').inc()
        winners = []
        
    # Remove winners older than 30 days and ensure they are dictionaries
    thirty_days_ago = int(time.time()) - 30 * 24 * 60 * 60
    winners = [winner for winner in winners if isinstance(winner, dict) and winner.get("timestamp", 0) > thirty_days_ago]

    abi = get_abi(contract_key, chain_name)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    event = contract.events.WinnerSelected()

    # The Gnosis node has a limit on the number of blocks to query
    # so we process events in chunks
    from_block = last_block + 1

    while from_block <= current_block:
        to_block = min(from_block + 10000, current_block)
        with print_lock:
            print(f"  - Scanning for WinnerSelected events from block {from_block} to {to_block}")

        try:
            for event_data in event.get_logs(from_block=from_block, to_block=to_block):
                owner = event_data.args.winner.owner
                stake = event_data.args.winner.stake
                block = w3.eth.get_block(event_data.blockNumber)
                timestamp = block.timestamp
                winners.append({"owner": owner, "stake": stake, "timestamp": timestamp})

            # Update last processed block number
            last_block_data[chain_name] = to_block
            with open(LAST_BLOCK_FILE, 'w') as f:
                json.dump(last_block_data, f)

            from_block = to_block + 1

        except Exception as e:
            with print_lock:
                print(f"    - Could not get events for blocks {from_block}-{to_block}: {e}")
            error_counter.labels(chain_name=chain_name, error_type='get_events').inc()
            # If we get an error, we skip this chunk and move to the next one
            from_block = to_block + 1
            last_block_data[chain_name] = from_block
            with open(LAST_BLOCK_FILE, 'w') as f:
                json.dump(last_block_data, f)
            time.sleep(1)

    # Save winners data
    try:
        with open(winners_file_path, 'w') as f:
            json.dump(winners, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save winners file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_winners_file').inc()

    # --- Process Winners ---
    
    weekly_winners = {}
    monthly_winners = {}
    all_time_winners = {}
    
    current_week = int(time.strftime("%U"))
    current_month = int(time.strftime("%m"))
    
    for winner in winners:
        owner = winner["owner"]
        stake = winner["stake"]
        
        # All time winners
        if owner in all_time_winners:
            all_time_winners[owner] += stake
        else:
            all_time_winners[owner] = stake
            
        # Weekly winners
        winner_week = int(time.strftime("%U", time.localtime(winner["timestamp"])))
        if winner_week == current_week:
            if owner in weekly_winners:
                weekly_winners[owner] += stake
            else:
                weekly_winners[owner] = stake
                
        # Monthly winners
        winner_month = int(time.strftime("%m", time.localtime(winner["timestamp"])))
        if winner_month == current_month:
            if owner in monthly_winners:
                monthly_winners[owner] += stake
            else:
                monthly_winners[owner] = stake
                
    # --- Weekly Winners ---
                
    # Save weekly winners data
    weekly_winners_file_path = os.path.join(DATA_DIR, f"winners-weekly_{chain_name}.json")
    try:
        with open(weekly_winners_file_path, 'w') as f:
            json.dump(weekly_winners, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save weekly winners file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_winners_weekly_file').inc()
        
    # Create leaderboard for weekly winners
    sorted_weekly_winners = sorted(weekly_winners.items(), key=lambda item: item[1], reverse=True)
    
    # Create Gauge for top 10 weekly winners
    metric_name_weekly = f"honeystats_{chain_name}_{contract_friendly_name}_winner_weekly_winnings"
    gauge_weekly = Gauge(
        metric_name_weekly,
        f"Total weekly winnings for top 10 winners of {contract_friendly_name} on {chain_config['name']}",
        ['owner'],
        registry=registry,
    )

    for i, (owner, total_stake) in enumerate(sorted_weekly_winners[:10]):
        stake_bzz = total_stake / (10**BZZ_DECIMALS)
        gauge_weekly.labels(owner=owner).set(stake_bzz)
        with print_lock:
            print(f"  - Top {i+1} weekly winner: {owner} with {stake_bzz} BZZ")
        
    # --- Monthly Winners ---
                
    # Save monthly winners data
    monthly_winners_file_path = os.path.join(DATA_DIR, f"winners-monthly_{chain_name}.json")
    try:
        with open(monthly_winners_file_path, 'w') as f:
            json.dump(monthly_winners, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save monthly winners file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_winners_monthly_file').inc()
        
    # Create leaderboard for monthly winners
    sorted_monthly_winners = sorted(monthly_winners.items(), key=lambda item: item[1], reverse=True)
    
    # Create Gauge for top 10 monthly winners
    metric_name_monthly = f"honeystats_{chain_name}_{contract_friendly_name}_winner_monthly_winnings"
    gauge_monthly = Gauge(
        metric_name_monthly,
        f"Total monthly winnings for top 10 winners of {contract_friendly_name} on {chain_config['name']}",
        ['owner'],
        registry=registry,
    )

    for i, (owner, total_stake) in enumerate(sorted_monthly_winners[:10]):
        stake_bzz = total_stake / (10**BZZ_DECIMALS)
        gauge_monthly.labels(owner=owner).set(stake_bzz)
        with print_lock:
            print(f"  - Top {i+1} monthly winner: {owner} with {stake_bzz} BZZ")
        
    # --- All Time Winners ---
            
    sorted_winners = sorted(all_time_winners.items(), key=lambda item: item[1], reverse=True)
    
    # Create Gauge for top 10 winners
    metric_name = f"honeystats_{chain_name}_{contract_friendly_name}_winner_total_winnings"
    gauge = Gauge(
        metric_name,
        f"Total winnings for top 10 winners of {contract_friendly_name} on {chain_config['name']}",
        ['owner'],
        registry=registry,
    )

    for i, (owner, total_stake) in enumerate(sorted_winners[:10]):
        stake_bzz = total_stake / (10**BZZ_DECIMALS)
        gauge.labels(owner=owner).set(stake_bzz)
        with print_lock:
            print(f"  - Top {i+1} winner: {owner} with {stake_bzz} BZZ")


def process_redistribution_events(registry, error_counter, chain_name):
    """Processes redistribution events from a given chain."""
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_config = chain_config["contracts"]["redistribution"]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    
    # We support multiple versions if they exist in the config
    versions = contract_config.get("versions", [{"address": contract_config["address"], "from_block": contract_config.get("deployment_block", 0), "to_block": None}])

    # Load event counts data (accumulated across all versions)
    event_counts_file_path = os.path.join(DATA_DIR, f"event_counts_redistribution_{chain_name}.json")
    try:
        with open(event_counts_file_path, 'r') as f:
            event_counts = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        event_counts = {}
    # Migration: ensure all required keys exist (old on-disk files predate committed/revealed)
    for _k in ("truth_selected", "price_adjustment_skipped", "withdraw_failed", "committed", "revealed"):
        event_counts.setdefault(_k, 0)

    truth_selected_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_truth_selected_total',
        'Total number of TruthSelected events in the redistribution game',
        registry=registry
    )
    price_adjustment_skipped_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_price_adjustment_skipped_total',
        'Total number of PriceAdjustmentSkipped events in the redistribution game',
        registry=registry
    )
    withdraw_failed_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_withdraw_failed_total',
        'Total number of WithdrawFailed events in the redistribution game',
        registry=registry
    )
    
    committed_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_committed_total',
        'Total number of Committed events in the redistribution game',
        registry=registry
    )

    revealed_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_revealed_total',
        'Total number of Revealed events in the redistribution game',
        registry=registry
    )
    
    commits_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_commits_total',
        'Total number of commits in the redistribution game',
        registry=registry
    )
    
    reveals_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_reveals_total',
        'Total number of reveals in the redistribution game',
        registry=registry
    )

    current_block = w3.eth.block_number

    for v_idx, version in enumerate(versions):
        contract_address = version["address"]
        from_block_config = version["from_block"]
        to_block_config = version["to_block"] if version["to_block"] else current_block
        
        v_label = f"v{v_idx}"
        with print_lock:
            print(f"Processing redistribution {v_label} ({contract_address}) on {chain_config['name']}...")

        # Load last processed block number for this version
        last_block_file_path = os.path.join(DATA_DIR, f"last_block_redistribution_{chain_name}_{v_label}.json")
        try:
            with open(last_block_file_path, 'r') as f:
                last_block_data = json.load(f)
                last_block = last_block_data.get(chain_name, from_block_config)
        except (FileNotFoundError, json.JSONDecodeError):
            last_block = from_block_config

        abi = get_abi("redistribution", chain_name)
        contract = w3.eth.contract(address=contract_address, abi=abi)

        scan_to_block = min(to_block_config, current_block)
        from_block = last_block + 1

        # Topic0 hashes for events that changed across versions
        TOPICS = {
            "truth_selected": [
                "0xd68bda4c8cfe73e460a4da48180fc20ddf81897751f902599953f9599a14187c", # v0-v4 (TruthSelected(bytes32))
                "0x34e8eda4cd857cd2865becf58a47748f31415f4a382cbb2cc0c64b9a27c717be"  # v5+ (TruthSelected(bytes32,uint8))
            ],
            "price_adjustment_skipped": [
                "0x20378e5d379eabfa30444ecc5eb2b87d0d77bdbf5a58d80d008673b0ca642141"
            ],
            "withdraw_failed": [
                "0x7ae187a0c04cf55b655ca83fa11d37854c882bf1fdcb588469b414731bb0e05a"
            ],
            "committed": [
                "0x68e0867601a98978930107aee7f425665e61edd70ca594c68ca5da9e81f84c29", # v0-v4
                "0xaadc88121471799d39ee2bbe1dd30a4ab57510e2a33bd6e537de5fafd2daa886"  # v5+
            ],
            "revealed": [
                "0x9ac97321502877edfdf86a17da1d2a38c12f7b1b6a6a40cd5e5f7261a50149fd", # v0-v4
                "0x13fc17fd71632266fe82092de6dd91a06b4fa68d8dc950492e5421cbed55a6a5"  # v5+
            ]
        }

        # Set initial values for gauges
        truth_selected_gauge.set(event_counts["truth_selected"])
        price_adjustment_skipped_gauge.set(event_counts["price_adjustment_skipped"])
        withdraw_failed_gauge.set(event_counts["withdraw_failed"])
        committed_gauge.set(event_counts.get("committed", 0))
        revealed_gauge.set(event_counts.get("revealed", 0))

        # Initial push to gateway
        try:
            push_to_gateway(PUSHGATEWAY_ADDRESS, job="honeystats", registry=registry)
        except:
            pass

        while from_block <= scan_to_block:
            to_block = min(from_block + 10000, scan_to_block)
            with print_lock:
                print(f"  - Scanning {v_label} for redistribution events from block {from_block} to {to_block}")

            try:
                # 1. Fetch Missing events via Topic0 for reliability across versions
                for event_type, topics in TOPICS.items():
                    for topic0 in topics:
                        logs = w3.eth.get_logs({
                            "address": contract_address,
                            "fromBlock": from_block,
                            "toBlock": to_block,
                            "topics": [topic0]
                        })
                        event_counts[event_type] += len(logs)

                # Update gauges incrementally
                truth_selected_gauge.set(event_counts["truth_selected"])
                price_adjustment_skipped_gauge.set(event_counts["price_adjustment_skipped"])
                withdraw_failed_gauge.set(event_counts["withdraw_failed"])
                committed_gauge.set(event_counts.get("committed", 0))
                revealed_gauge.set(event_counts.get("revealed", 0))

                # Push to gateway incrementally
                try:
                    push_to_gateway(PUSHGATEWAY_ADDRESS, job="honeystats", registry=registry)
                except:
                    pass

                # 2. Handle Round-based gauges
                try:
                    for event_data in contract.events.CountCommits.get_logs(from_block=from_block, to_block=to_block):
                        commits_gauge.set(event_data.args._count)
                    for event_data in contract.events.CountReveals.get_logs(from_block=from_block, to_block=to_block):
                        reveals_gauge.set(event_data.args._count)
                except:
                    pass

                # Save last processed block number
                with open(last_block_file_path, 'w') as f:
                    json.dump({chain_name: to_block}, f)

                # Save event counts
                with open(event_counts_file_path, 'w') as f:
                    json.dump(event_counts, f)

                from_block = to_block + 1

            except Exception as e:
                with print_lock:
                    print(f"    - Could not get redistribution {v_label} events for blocks {from_block}-{to_block}: {e}")
                error_counter.labels(chain_name=chain_name, error_type=f'get_redistribution_{v_label}_events').inc()
                from_block = to_block + 1
                time.sleep(1)

    # Set event count gauges
    truth_selected_gauge.set(event_counts["truth_selected"])
    price_adjustment_skipped_gauge.set(event_counts["price_adjustment_skipped"])
    withdraw_failed_gauge.set(event_counts["withdraw_failed"])
    committed_gauge.set(event_counts.get("committed", 0))
    revealed_gauge.set(event_counts.get("revealed", 0))


def process_postagestamp_events(registry, error_counter, chain_name):
    """Processes postagestamp events from a given chain to track rented capacity."""
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_config = chain_config["contracts"]["postagestamp"]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    
    # We support multiple versions if they exist in the config
    versions = contract_config.get("versions", [{"address": contract_config["address"], "from_block": contract_config.get("deployment_block", 0), "to_block": None}])

    total_capacity_bytes = 0
    total_active_batches = 0
    total_ttl_sum = 0
    total_min_ttl = float("inf")
    total_expiring_soon_bytes = 0
    global_owner_capacity = {}

    rented_capacity_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_rented_capacity_tb",
        "Total active rented capacity in TB",
        registry=registry,
    )

    active_batches_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_active_batches_total",
        "Total number of active (non-expired) postage batches",
        registry=registry,
    )

    min_ttl_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_min_ttl_blocks",
        "Minimum TTL remaining among all active batches (blocks)",
        registry=registry,
    )

    avg_ttl_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_avg_ttl_blocks",
        "Average TTL remaining among all active batches (blocks)",
        registry=registry,
    )

    expiring_soon_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_expiring_30d_capacity_tb",
        "Capacity expiring within the next 30 days (TB)",
        registry=registry,
    )

    owner_capacity_gauge = Gauge(
        f"honeystats_{chain_name}_postagestamp_owner_capacity_tb",
        "Active capacity owned by address (TB)",
        ["owner"],
        registry=registry,
    )

    current_block = w3.eth.block_number
    # Approximation for 30 days in blocks (Gnosis ~5s, Sepolia ~12s)
    blocks_in_30d = (30 * 24 * 60 * 60) // (5 if chain_name == "gnosis" else 12)

    for v_idx, version in enumerate(versions):
        contract_address = version["address"]
        from_block_config = version["from_block"]
        to_block_config = (
            version["to_block"] if version["to_block"] else current_block
        )

        v_label = f"v{v_idx}"
        with print_lock:
            print(
                f"Processing postagestamp {v_label} ({contract_address}) on {chain_config['name']}..."
            )

        # Load last processed block number for this version
        last_block_file_path = os.path.join(
            DATA_DIR, f"last_block_postagestamp_{chain_name}_{v_label}.json"
        )
        try:
            with open(last_block_file_path, "r") as f:
                last_block_data = json.load(f)
                last_block = last_block_data.get(chain_name, from_block_config)
        except (FileNotFoundError, json.JSONDecodeError):
            last_block = from_block_config

        # Load batches data for this version
        batches_file_path = os.path.join(
            DATA_DIR, f"batches_{chain_name}_{v_label}.json"
        )
        try:
            with open(batches_file_path, "r") as f:
                batches = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            batches = {}

        # Load price updates for this version
        price_updates_file_path = os.path.join(
            DATA_DIR, f"price_updates_{chain_name}_{v_label}.json"
        )
        try:
            with open(price_updates_file_path, "r") as f:
                price_updates = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            price_updates = []

        abi = get_abi("postagestamp", chain_name)
        contract = w3.eth.contract(address=contract_address, abi=abi)

        scan_to_block = min(to_block_config, current_block)
        from_block = last_block + 1

        while from_block <= scan_to_block:
            to_block = min(from_block + 10000, scan_to_block)
            with print_lock:
                print(
                    f"  - Scanning {v_label} for postagestamp events from block {from_block} to {to_block}"
                )

            try:
                for event_data in contract.events.BatchCreated.get_logs(
                    from_block=from_block, to_block=to_block
                ):
                    batch_id = "0x" + event_data.args.batchId.hex()
                    batches[batch_id] = {
                        "depth": event_data.args.depth,
                        "owner": event_data.args.owner,
                        "normalisedBalance": event_data.args.normalisedBalance,
                    }

                for event_data in contract.events.BatchTopUp.get_logs(
                    from_block=from_block, to_block=to_block
                ):
                    batch_id = "0x" + event_data.args.batchId.hex()
                    if batch_id in batches:
                        batches[batch_id][
                            "normalisedBalance"
                        ] = event_data.args.normalisedBalance

                for event_data in contract.events.BatchDepthIncrease.get_logs(
                    from_block=from_block, to_block=to_block
                ):
                    batch_id = "0x" + event_data.args.batchId.hex()
                    if batch_id in batches:
                        batches[batch_id]["depth"] = event_data.args.newDepth
                        batches[batch_id][
                            "normalisedBalance"
                        ] = event_data.args.normalisedBalance

                for event_data in contract.events.PriceUpdate.get_logs(
                    from_block=from_block, to_block=to_block
                ):
                    price_updates.append(
                        {
                            "block": event_data.blockNumber,
                            "price": event_data.args.price,
                        }
                    )

                # Save last processed block number
                with open(last_block_file_path, "w") as f:
                    json.dump({chain_name: to_block}, f)

                from_block = to_block + 1

            except Exception as e:
                with print_lock:
                    print(
                        f"    - Could not get postagestamp {v_label} events for blocks {from_block}-{to_block}: {e}"
                    )
                error_counter.labels(
                    chain_name=chain_name,
                    error_type=f"get_postagestamp_{v_label}_events",
                ).inc()
                from_block = to_block + 1
                time.sleep(1)

        # Save batches and price updates
        with open(batches_file_path, "w") as f:
            json.dump(batches, f)
        with open(price_updates_file_path, "w") as f:
            json.dump(price_updates, f)

        # Calculate active capacity for this version
        current_outpayment = get_current_outpayment(price_updates, scan_to_block)
        current_price = price_updates[-1]["price"] if price_updates else 0

        version_capacity_bytes = 0
        version_active_batches = 0

        for batch in batches.values():
            if batch["normalisedBalance"] > current_outpayment:
                capacity = (2 ** batch["depth"]) * 4096
                version_capacity_bytes += capacity
                version_active_batches += 1

                # TTL Calculation
                if current_price > 0:
                    ttl = (
                        batch["normalisedBalance"] - current_outpayment
                    ) // current_price
                    total_ttl_sum += ttl
                    if ttl < total_min_ttl:
                        total_min_ttl = ttl

                    if ttl < blocks_in_30d:
                        total_expiring_soon_bytes += capacity

                # Owner tracking
                owner = batch["owner"]
                global_owner_capacity[owner] = (
                    global_owner_capacity.get(owner, 0) + capacity
                )

        total_capacity_bytes += version_capacity_bytes if version["to_block"] is None else 0
        total_active_batches += version_active_batches if version["to_block"] is None else 0

    # Final Gauge Updates
    total_capacity_tb = total_capacity_bytes / (10**12)
    rented_capacity_gauge.set(total_capacity_tb)
    active_batches_gauge.set(total_active_batches)

    if total_active_batches > 0:
        avg_ttl_gauge.set(total_ttl_sum / total_active_batches)
        min_ttl_gauge.set(total_min_ttl if total_min_ttl != float("inf") else 0)

    expiring_soon_gauge.set(total_expiring_soon_bytes / (10**12))

    # Set top 10 owners
    sorted_owners = sorted(
        global_owner_capacity.items(), key=lambda x: x[1], reverse=True
    )
    for owner, cap_bytes in sorted_owners[:10]:
        owner_capacity_gauge.labels(owner=owner).set(cap_bytes / (10**12))


def process_staking_events(registry, error_counter, chain_name):
    """Processes staking events from a given chain."""
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_key = "staking"
    contract_config = chain_config["contracts"][contract_key]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    contract_address = contract_config["address"]

    with print_lock:
        print(f"Processing staking events for {contract_friendly_name} on {chain_config['name']}...")

    # Load last processed block number
    current_block = w3.eth.block_number
    last_block_file_path = os.path.join(DATA_DIR, f"last_block_staking_{chain_name}.json")
    last_block_data = {}
    try:
        with open(last_block_file_path, 'r') as f:
            last_block_data = json.load(f)
            last_block = last_block_data.get(chain_name, contract_config.get("deployment_block", 0))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load last block file for staking events: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_last_block_staking').inc()
        last_block = contract_config.get("deployment_block", 0)
        last_block_data[chain_name] = last_block
        with open(last_block_file_path, 'w') as f:
            json.dump(last_block_data, f)
            
    # Load stakers data
    stakers_file_path = os.path.join(DATA_DIR, f"stakers_{chain_name}.json")
    try:
        with open(stakers_file_path, 'r') as f:
            stakers = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load stakers file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_stakers_file').inc()
        stakers = {}

    # Load frozen stakers data
    frozen_stakers_file_path = os.path.join(DATA_DIR, f"frozen_stakers_{chain_name}.json")
    try:
        with open(frozen_stakers_file_path, 'r') as f:
            frozen_stakers = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load frozen stakers file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_frozen_stakers_file').inc()
        frozen_stakers = []

    # Load event counts data
    event_counts_file_path = os.path.join(DATA_DIR, f"event_counts_staking_{chain_name}.json")
    try:
        with open(event_counts_file_path, 'r') as f:
            event_counts = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load event counts file: {e}")
        event_counts = {"stake_slashed": 0, "stake_frozen": 0, "stake_updated": 0}

    abi = get_abi(contract_key, chain_name)
    contract = w3.eth.contract(address=contract_address, abi=abi)

    stake_slashed_gauge = Gauge(
        f'honeystats_{chain_name}_staking_stake_slashed_total',
        'Total number of StakeSlashed events in the staking contract',
        registry=registry
    )
    stake_frozen_gauge = Gauge(
        f'honeystats_{chain_name}_staking_stake_frozen_total',
        'Total number of StakeFrozen events in the staking contract',
        registry=registry
    )
    stake_updated_gauge = Gauge(
        f'honeystats_{chain_name}_staking_stake_updated_total',
        'Total number of StakeUpdated events in the staking contract',
        registry=registry
    )
    
    stakers_gauge = Gauge(
        f'honeystats_{chain_name}_staking_stakers_total',
        'Total number of stakers in the staking contract',
        registry=registry
    )
    
    total_stake_gauge = Gauge(
        f'honeystats_{chain_name}_staking_total_stake',
        'Total stake in the staking contract',
        registry=registry
    )

    frozen_stake_gauge = Gauge(
        f'honeystats_{chain_name}_staking_frozen_stake_amount',
        'Total amount of frozen stake in the staking contract',
        registry=registry
    )

    from_block = last_block + 1

    with print_lock:
        print(f"Scanning for staking events from block {from_block} to {current_block}...")

    while from_block <= current_block:
        to_block = min(from_block + 10000, current_block)
        with print_lock:
            print(f"  - Scanning for staking events from block {from_block} to {to_block}")

        try:
            for event_data in contract.events.StakeSlashed.get_logs(from_block=from_block, to_block=to_block):
                event_counts["stake_slashed"] += 1
                owner = event_data.args.slashed
                if owner in frozen_stakers:
                    frozen_stakers.remove(owner)

            for event_data in contract.events.StakeFrozen.get_logs(from_block=from_block, to_block=to_block):
                event_counts["stake_frozen"] += 1
                owner = event_data.args.frozen
                if owner not in frozen_stakers:
                    frozen_stakers.append(owner)

            for event_data in contract.events.StakeUpdated.get_logs(from_block=from_block, to_block=to_block):
                event_counts["stake_updated"] += 1
                owner = event_data.args.owner
                stake = event_data.args.committedStake
                stakers[owner] = stake

            for event_data in contract.events.StakeWithdrawn.get_logs(from_block=from_block, to_block=to_block):
                owner = event_data.args.node
                if owner in frozen_stakers:
                    frozen_stakers.remove(owner)

            # Save last processed block number
            with open(last_block_file_path, 'w') as f:
                json.dump({chain_name: to_block}, f)

            # Save stakers data
            try:
                with open(stakers_file_path, 'w') as f:
                    json.dump(stakers, f)
            except Exception as e:
                with print_lock:
                    print(f"    - Could not save stakers file: {e}")
                error_counter.labels(chain_name=chain_name, error_type='write_stakers_file').inc()
            
            # Save frozen stakers data
            try:
                with open(frozen_stakers_file_path, 'w') as f:
                    json.dump(frozen_stakers, f)
            except Exception as e:
                with print_lock:
                    print(f"    - Could not save frozen stakers file: {e}")
                error_counter.labels(chain_name=chain_name, error_type='write_frozen_stakers_file').inc()

            # Save event counts
            try:
                with open(event_counts_file_path, 'w') as f:
                    json.dump(event_counts, f)
            except Exception as e:
                with print_lock:
                    print(f"    - Could not save event counts file: {e}")
                error_counter.labels(chain_name=chain_name, error_type='write_event_counts_file').inc()

            stakers_gauge.set(len(stakers))

            total_stake = sum(stakers.values()) if stakers else 0
            total_stake_gauge.set(total_stake / (10**BZZ_DECIMALS))

            total_frozen_stake = 0
            for owner in frozen_stakers:
                if owner in stakers:
                    total_frozen_stake += stakers[owner]
            frozen_stake_gauge.set(total_frozen_stake / (10**BZZ_DECIMALS))

            stake_slashed_gauge.set(event_counts["stake_slashed"])
            stake_frozen_gauge.set(event_counts["stake_frozen"])
            stake_updated_gauge.set(event_counts["stake_updated"])

            from_block = to_block + 1

        except Exception as e:
            with print_lock:
                print(f"    - Could not get staking events for blocks {from_block}-{to_block}: {e}")
            error_counter.labels(chain_name=chain_name, error_type='get_staking_events').inc()
            # If we get an error, we skip this chunk and move to the next one
            from_block = to_block + 1
            with open(last_block_file_path, 'w') as f:
                json.dump({chain_name: from_block}, f)
            time.sleep(1)
    
    # Save stakers data
    try:
        with open(stakers_file_path, 'w') as f:
            json.dump(stakers, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save stakers file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_stakers_file').inc()

    # Save frozen stakers data
    try:
        with open(frozen_stakers_file_path, 'w') as f:
            json.dump(frozen_stakers, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save frozen stakers file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_frozen_stakers_file').inc()

    # Save event counts
    try:
        with open(event_counts_file_path, 'w') as f:
            json.dump(event_counts, f)
    except Exception as e:
        with print_lock:
            print(f"    - Could not save event counts file: {e}")
        error_counter.labels(chain_name=chain_name, error_type='write_event_counts_file').inc()

    stakers_gauge.set(len(stakers))

    total_stake = sum(stakers.values()) if stakers else 0
    total_stake_gauge.set(total_stake / (10**BZZ_DECIMALS))

    total_frozen_stake = 0
    for owner in frozen_stakers:
        if owner in stakers:
            total_frozen_stake += stakers[owner]
    frozen_stake_gauge.set(total_frozen_stake / (10**BZZ_DECIMALS))

    stake_slashed_gauge.set(event_counts["stake_slashed"])
    stake_frozen_gauge.set(event_counts["stake_frozen"])
    stake_updated_gauge.set(event_counts["stake_updated"])


import threading

def main(registry):
    """Main function to query contracts and push metrics."""

    redistribution_errors = Counter(
        'honeystats_redistribution_errors_total',
        'Total number of errors in the redistribution game',
        ['chain_name', 'error_type'],
        registry=registry
    )

    # Create all gauges for contract metrics at the beginning
    gauges = {}
    for chain_name, chain_config in CHAINS.items():
        for contract_key, contract_config in chain_config["contracts"].items():
            contract_friendly_name = contract_config["name"].replace(" ", "_")
            abi = get_abi(contract_key, chain_name)
            contract = Web3(Web3.HTTPProvider(chain_config["rpc_url"])).eth.contract(address=contract_config["address"], abi=abi)
            for func in contract.all_functions():
                if (
                    func.abi["type"] == "function"
                    and func.abi["stateMutability"] in ("view", "pure")
                    and not func.abi.get("inputs", [])
                    and is_simple_output(func.abi.get("outputs", []))
                ):
                    metric_name = f"honeystats_{chain_name}_{contract_friendly_name}_{func.fn_name}"
                    gauges[metric_name] = Gauge(
                        metric_name,
                        f"Value of {func.fn_name} for {contract_friendly_name} on {chain_config['name']}",
                        registry=registry,
                    )

    threads = []
    # --- Process Events in parallel ---
    winner_thread = threading.Thread(target=process_winner_events, args=(registry, redistribution_errors))
    threads.append(winner_thread)
    winner_thread.start()
    
    for chain_name in CHAINS:
        redistribution_thread = threading.Thread(target=process_redistribution_events, args=(registry, redistribution_errors, chain_name))
        threads.append(redistribution_thread)
        redistribution_thread.start()
    
        staking_thread = threading.Thread(target=process_staking_events, args=(registry, redistribution_errors, chain_name))
        threads.append(staking_thread)
        staking_thread.start()

        postagestamp_thread = threading.Thread(target=process_postagestamp_events, args=(registry, redistribution_errors, chain_name))
        threads.append(postagestamp_thread)
        postagestamp_thread.start()

    for thread in threads:
        thread.join()

    # --- Query Contract Metrics ---
    for chain_name, chain_config in CHAINS.items():
        with print_lock:
            print(f"Querying {chain_config['name']}...")
        w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

        for contract_key, contract_config in chain_config["contracts"].items():
            contract_friendly_name = contract_config["name"].replace(" ", "_")
            contract_address = contract_config["address"]
            with print_lock:
                print(f"  - Contract: {contract_config['name']} ({contract_address})")

            try:
                abi = get_abi(contract_key, chain_name)
                contract = w3.eth.contract(address=contract_address, abi=abi)

                for func in contract.all_functions():
                    if (
                        func.abi["type"] == "function"
                        and func.abi["stateMutability"] in ("view", "pure")
                        and not func.abi.get("inputs", [])
                        and is_simple_output(func.abi.get("outputs", []))
                    ):
                        try:
                            value = func().call()
                            
                            if contract_friendly_name == "PriceOracle" and func.fn_name == "currentPrice":
                                try:
                                    price_base = contract.functions.priceBase().call()
                                    if price_base > 0:
                                        value = value / price_base
                                except Exception as e:
                                    with print_lock:
                                        print(f"    - Could not get priceBase for PriceOracle: {e}")

                            metric_name = f"honeystats_{chain_name}_{contract_friendly_name}_{func.fn_name}"
                            
                            bzz_denominated_metrics = {
                                "BzzToken": ["totalSupply"],
                                "PostageStamp": ["currentTotalOutPayment", "pot", "minimumInitialBalancePerChunk", "lastExpiryBalance"],
                                "Staking": ["withdrawableStake"]
                            }
                            
                            if contract_friendly_name in bzz_denominated_metrics and func.fn_name in bzz_denominated_metrics[contract_friendly_name]:
                                if isinstance(value, (int, float)):
                                    gauges[metric_name].set(value / (10**BZZ_DECIMALS))
                            elif isinstance(value, (int, float)):
                                gauges[metric_name].set(value)
                            elif isinstance(value, bool):
                                gauges[metric_name].set(1 if value else 0)
                        except Exception as e:
                            with print_lock:
                                print(f"    - Could not call {func.fn_name}(): {e}")

            except Exception as e:
                with print_lock:
                    print(f"    - Could not process contract {contract_friendly_name}: {e}")

    try:
        with print_lock:
            push_to_gateway(
                PUSHGATEWAY_ADDRESS, job="honeystats", registry=registry
            )
            print("Successfully pushed metrics to Pushgateway.")
    except Exception as e:
        with print_lock:
            print(f"Could not push metrics to Pushgateway: {e}")

if __name__ == "__main__":
    print("Starting honeystats...")
    while True:
        registry = CollectorRegistry()
        main(registry)
        print("Finished honeystats run. Waiting 1 minute for the next run...")
        time.sleep(60)
