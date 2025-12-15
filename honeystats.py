
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
        "rpc_url": "https://sep.swarm1.ethswarm.org",
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
        "rpc_url": "https://gno.prod.ethswarm.org",
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
            },
            "staking": {
                "name": "Staking",
                "address": "0xda2a16EE889E7F04980A8d597b48c8D51B9518F4",
                "deployment_block": 40430237,
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
    print(f"Starting redistribution event processing for {chain_name}...")
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_key = "redistribution"
    contract_config = chain_config["contracts"][contract_key]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    contract_address = contract_config["address"]

    with print_lock:
        print(f"Processing redistribution events for {contract_friendly_name} on {chain_config['name']}...")

    # Load last processed block number
    current_block = w3.eth.block_number
    last_block_file_path = os.path.join(DATA_DIR, f"last_block_redistribution_{chain_name}.json")
    last_block_data = {}
    try:
        with open(last_block_file_path, 'r') as f:
            last_block_data = json.load(f)
            last_block = last_block_data.get(chain_name, contract_config.get("deployment_block", 0))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        with print_lock:
            print(f"    - Could not load last block file for redistribution events: {e}")
        error_counter.labels(chain_name=chain_name, error_type='read_last_block_redistribution').inc()
        last_block = contract_config.get("deployment_block", 0)
        last_block_data[chain_name] = last_block
        with open(last_block_file_path, 'w') as f:
            json.dump(last_block_data, f)

    abi = get_abi(contract_key, chain_name)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    
    truth_selected_counter = Counter(
        f'honeystats_{chain_name}_redistribution_truth_selected_total',
        'Total number of TruthSelected events in the redistribution game',
        registry=registry
    )
    price_adjustment_skipped_counter = Counter(
        f'honeystats_{chain_name}_redistribution_price_adjustment_skipped_total',
        'Total number of PriceAdjustmentSkipped events in the redistribution game',
        registry=registry
    )
    withdraw_failed_counter = Counter(
        f'honeystats_{chain_name}_redistribution_withdraw_failed_total',
        'Total number of WithdrawFailed events in the redistribution game',
        registry=registry
    )
    
    commits_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_commits_total',
        'Total number of commits in the redistribution game',
        ['round'],
        registry=registry
    )
    
    reveals_gauge = Gauge(
        f'honeystats_{chain_name}_redistribution_reveals_total',
        'Total number of reveals in the redistribution game',
        ['round'],
        registry=registry
    )

    from_block = last_block + 1

    while from_block <= current_block:
        to_block = min(from_block + 10000, current_block)
        with print_lock:
            print(f"  - Scanning for redistribution events from block {from_block} to {to_block}")

        try:
            current_round = contract.functions.currentRound().call()
            with print_lock:
                print(f"  - Current round for redistribution on {chain_name} is {current_round}")

            for event_data in contract.events.TruthSelected.get_logs(from_block=from_block, to_block=to_block):
                with print_lock:
                    print(f"Found TruthSelected event in block {event_data.blockNumber}")
                truth_selected_counter.inc()
            
            for event_data in contract.events.PriceAdjustmentSkipped.get_logs(from_block=from_block, to_block=to_block):
                with print_lock:
                    print(f"Found PriceAdjustmentSkipped event in block {event_data.blockNumber}")
                price_adjustment_skipped_counter.inc()

            for event_data in contract.events.WithdrawFailed.get_logs(from_block=from_block, to_block=to_block):
                with print_lock:
                    print(f"Found WithdrawFailed event in block {event_data.blockNumber}")
                withdraw_failed_counter.inc()

            found_commits_event = False
            for event_data in contract.events.CountCommits.get_logs(from_block=from_block, to_block=to_block):
                with print_lock:
                    print(f"Found CountCommits event in block {event_data.blockNumber} with count {event_data.args._count}")
                commits_gauge.labels(round=current_round).set(event_data.args._count)
                found_commits_event = True
            
            if not found_commits_event:
                with print_lock:
                    print(f"  - No CountCommits events found for blocks {from_block}-{to_block}")

            found_reveals_event = False
            for event_data in contract.events.CountReveals.get_logs(from_block=from_block, to_block=to_block):
                with print_lock:
                    print(f"Found CountReveals event in block {event_data.blockNumber} with count {event_data.args._count}")
                reveals_gauge.labels(round=current_round).set(event_data.args._count)
                found_reveals_event = True

            if not found_reveals_event:
                with print_lock:
                    print(f"  - No CountReveals events found for blocks {from_block}-{to_block}")

            # Save last processed block number
            with open(last_block_file_path, 'w') as f:
                json.dump({chain_name: to_block}, f)

            from_block = to_block + 1

        except Exception as e:
            with print_lock:
                print(f"    - Could not get redistribution events for blocks {from_block}-{to_block}: {e}")
            error_counter.labels(chain_name=chain_name, error_type='get_redistribution_events').inc()
            # If we get an error, we skip this chunk and move to the next one
            from_block = to_block + 1
            with open(last_block_file_path, 'w') as f:
                json.dump({chain_name: from_block}, f)
            time.sleep(1)


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

    abi = get_abi(contract_key, chain_name)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    
    stake_slashed_counter = Counter(
        f'honeystats_{chain_name}_staking_stake_slashed_total',
        'Total number of StakeSlashed events in the staking contract',
        registry=registry
    )
    stake_frozen_counter = Counter(
        f'honeystats_{chain_name}_staking_stake_frozen_total',
        'Total number of StakeFrozen events in the staking contract',
        registry=registry
    )
    stake_updated_counter = Counter(
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

    from_block = last_block + 1

    with print_lock:
        print(f"Scanning for staking events from block {from_block} to {current_block}...")

    while from_block <= current_block:
        to_block = min(from_block + 10000, current_block)
        with print_lock:
            print(f"  - Scanning for staking events from block {from_block} to {to_block}")

        try:
            for event_data in contract.events.StakeSlashed.get_logs(from_block=from_block, to_block=to_block):
                stake_slashed_counter.inc()
            
            for event_data in contract.events.StakeFrozen.get_logs(from_block=from_block, to_block=to_block):
                stake_frozen_counter.inc()
            
            for event_data in contract.events.StakeUpdated.get_logs(from_block=from_block, to_block=to_block):
                stake_updated_counter.inc()
                owner = event_data.args.owner
                stake = event_data.args.committedStake
                stakers[owner] = stake

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
            
            stakers_gauge.set(len(stakers))
            
            total_stake = sum(stakers.values()) if stakers else 0
            total_stake_gauge.set(total_stake / (10**BZZ_DECIMALS))

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
    
    stakers_gauge.set(len(stakers))
    
    total_stake = sum(stakers.values()) if stakers else 0
    total_stake_gauge.set(total_stake / (10**BZZ_DECIMALS))


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
