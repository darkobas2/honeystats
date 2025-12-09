
import os
import time
import json
from web3 import Web3
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

# --- Configuration ---

PUSHGATEWAY_ADDRESS = os.environ.get("PUSHGATEWAY_ADDRESS", "https://pgw.godfather2.ethswarm.org")
LAST_BLOCK_FILE = "last_block.json"
WINNERS_FILE = "winners.json"

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
            },
            "redistribution": {
                "name": "Redistribution",
                "address": "0x5b718E36F5Ce2F2F7e25A397040436Ce6af3e89e",
            },
            "postagestamp": {
                "name": "PostageStamp",
                "address": "0xcdfdC3752caaA826fE62531E0000C40546eC56A6",
            },
            "priceoracle": {
                "name": "PriceOracle",
                "address": "0x95Dc18380e92C13E4F8a4e94C99FB1b97250174B",
            },
            "staking": {
                "name": "Staking",
                "address": "0xEEF13Ef9eD9cDD169701eeF3cd832df298dD1bB4",
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
            },
            "redistribution": {
                "name": "Redistribution",
                "address": "0x5069cdfB3D9E56d23B1cAeE83CE6109A7E4fd62d",
            },
            "priceoracle": {
                "name": "PriceOracle",
                "address": "0x47EeF336e7fE5bED98499A4696bce8f28c1B0a8b",
            },
            "postagestamp": {
                "name": "PostageStamp",
                "address": "0x45a1502382541Cd610CC9068e88727426b696293",
            },
            "staking": {
                "name": "Staking",
                "address": "0xda2a16EE889E7F04980A8d597b48c8D51B9518F4",
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


def process_winner_events(registry):
    """Processes winner events from the Gnosis chain and creates a leaderboard."""
    chain_name = "gnosis"
    chain_config = CHAINS[chain_name]
    w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

    contract_key = "redistribution"
    contract_config = chain_config["contracts"][contract_key]
    contract_friendly_name = contract_config["name"].replace(" ", "_")
    contract_address = contract_config["address"]

    print(f"Processing winner events for {contract_friendly_name} on {chain_config['name']}...")

    # Load last processed block number
    try:
        with open(LAST_BLOCK_FILE, 'r') as f:
            last_block_data = json.load(f)
            last_block = last_block_data.get(chain_name, 0)
    except (FileNotFoundError, json.JSONDecodeError):
        last_block = 0

    # Load winners data
    try:
        with open(WINNERS_FILE, 'r') as f:
            winners = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        winners = {}

    abi = get_abi(contract_key, chain_name)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    event = contract.events.WinnerSelected()

    # The Gnosis node has a limit on the number of blocks to query
    # so we process events in chunks
    current_block = w3.eth.block_number
    from_block = last_block + 1

    while from_block <= current_block:
        to_block = min(from_block + 10000, current_block)
        print(f"  - Scanning for WinnerSelected events from block {from_block} to {to_block}")

        try:
            for event_data in event.get_logs(from_block=from_block, to_block=to_block):
                owner = event_data.args.owner
                stake = event_data.args.stake

                if owner in winners:
                    winners[owner] += stake
                else:
                    winners[owner] = stake

            # Save last processed block number
            with open(LAST_BLOCK_FILE, 'w') as f:
                json.dump({chain_name: to_block}, f)

            from_block = to_block + 1

        except Exception as e:
            print(f"    - Could not get events for blocks {from_block}-{to_block}: {e}")
            # If we get an error, we try a smaller chunk
            to_block = min(from_block + 1000, current_block)
            if from_block >= to_block:
                # If we are already at the smallest chunk, we stop
                break

    # Save winners data
    with open(WINNERS_FILE, 'w') as f:
        json.dump(winners, f)

    # Create leaderboard
    sorted_winners = sorted(winners.items(), key=lambda item: item[1], reverse=True)
    
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
        print(f"  - Top {i+1} winner: {owner} with {stake_bzz} BZZ")


def main():
    """Main function to query contracts and push metrics."""
    registry = CollectorRegistry()

    # --- Process Gnosis Winner Events ---
    try:
        process_winner_events(registry)
    except Exception as e:
        print(f"Could not process winner events: {e}")
    # --- End Process Gnosis Winner Events ---

    for chain_name, chain_config in CHAINS.items():
        print(f"Querying {chain_config['name']}...")
        w3 = Web3(Web3.HTTPProvider(chain_config["rpc_url"]))

        for contract_key, contract_config in chain_config["contracts"].items():
            contract_friendly_name = contract_config["name"].replace(" ", "_")
            contract_address = contract_config["address"]
            print(f"  - Contract: {contract_config['name']} ({contract_address})")

            try:
                abi = get_abi(contract_key, chain_name)
                contract = w3.eth.contract(address=contract_address, abi=abi)

                for func in contract.all_functions():
                    if (
                        func.abi["type"] == "function"
                        and func.abi["stateMutability"] in ("view", "pure")
                        and not func.abi.get("inputs", [])
                    ):
                        if func.fn_name == 'winner':
                            try:
                                winner_data = func().call()
                                if all(winner_data): # Ensure winner_data is not empty
                                    overlay, owner, depth, stake, stake_density, _ = winner_data
                                    
                                    stake_bzz = stake / (10**BZZ_DECIMALS)
                                    stake_density_bzz = stake_density / (10**BZZ_DECIMALS)

                                    for key, value in {"depth": depth, "stake": stake_bzz, "stake_density": stake_density_bzz}.items():
                                        metric_name = f"honeystats_{chain_name}_{contract_friendly_name}_winner_{key}"
                                        gauge = Gauge(
                                            metric_name,
                                            f"Winner {key} for {contract_friendly_name} on {chain_config['name']}",
                                            ['overlay', 'owner'],
                                            registry=registry,
                                        )
                                        gauge.labels(overlay=overlay.hex(), owner=owner).set(value)
                            except Exception as e:
                                print(f"    - Could not call {func.fn_name}(): {e}")
                            continue

                        if is_simple_output(func.abi.get("outputs", [])):
                            try:
                                value = func().call()
                                
                                if contract_friendly_name == "PriceOracle" and func.fn_name == "currentPrice":
                                    try:
                                        price_base = contract.functions.priceBase().call()
                                        if price_base > 0:
                                            value = value / price_base
                                    except Exception as e:
                                        print(f"    - Could not get priceBase for PriceOracle: {e}")

                                metric_name = f"honeystats_{chain_name}_{contract_friendly_name}_{func.fn_name}"
                                gauge = Gauge(
                                    metric_name,
                                    f"Value of {func.fn_name} for {contract_friendly_name} on {chain_config['name']}",
                                    registry=registry,
                                )
                                
                                bzz_denominated_metrics = {
                                    "BzzToken": ["totalSupply"],
                                    "PostageStamp": ["currentTotalOutPayment", "pot", "minimumInitialBalancePerChunk", "lastExpiryBalance"],
                                    "Staking": ["withdrawableStake"]
                                }
                                
                                if contract_friendly_name in bzz_denominated_metrics and func.fn_name in bzz_denominated_metrics[contract_friendly_name]:
                                    if isinstance(value, (int, float)):
                                        gauge.set(value / (10**BZZ_DECIMALS))
                                elif isinstance(value, (int, float)):
                                    gauge.set(value)
                                elif isinstance(value, bool):
                                    gauge.set(1 if value else 0)
                                print(f"    - {func.fn_name}(): {value}")
                            except Exception as e:
                                print(f"    - Could not call {func.fn_name}(): {e}")

            except Exception as e:
                print(f"    - Could not process contract {contract_friendly_name}: {e}")

    try:
        push_to_gateway(
            PUSHGATEWAY_ADDRESS, job="honeystats", registry=registry
        )
        print("Successfully pushed metrics to Pushgateway.")
    except Exception as e:
        print(f"Could not push metrics to Pushgateway: {e}")


if __name__ == "__main__":
    print("Starting honeystats...")
    while True:
        main()
        print("Finished honeystats run. Waiting 1 minute for the next run...")
        time.sleep(60)
