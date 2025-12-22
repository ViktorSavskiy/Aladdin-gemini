import pandas as pd
from datetime import datetime
from config.settings import Config

class PortfolioReportGenerator:
    """Генерация отчета по портфелю"""
    
    @staticmethod
    def generate_rebalance_report(comparison_df: pd.DataFrame, orders: list, stats: dict):
        try:
            path = Config.DATA_DIR / "reports" / "portfolio_action_plan.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                f.write(f"PORTFOLIO ACTION PLAN | {datetime.now()}\n")
                f.write("="*60 + "\n\n")
                
                f.write("📊 CURRENT STATUS:\n")
                f.write(f"Total Value: ${stats.get('total_value_usd', 0):.2f}\n")
                f.write(f"Health Score: {stats.get('aladdin_health_score', 0):.1f} / 100\n")
                f.write(f"Assets: {stats.get('asset_count', 0)}\n\n")
                
                f.write("⚖️ DEVIATION ANALYSIS:\n")
                f.write(f"{'Symbol':<8} {'Cur. W%':<8} {'Tgt. W%':<8} {'Delta USD':<12} {'Action'}\n")
                f.write("-" * 60 + "\n")
                
                # Сортируем: сначала Sell, потом Buy
                sorted_df = comparison_df.sort_values('value_delta', ascending=True)
                
                for _, row in sorted_df.iterrows():
                    sym = row['symbol']
                    cw = row['current_weight'] * 100
                    tw = row['target_weight'] * 100
                    delta = row['value_delta']
                    act = row['action']
                    
                    if act == 'HOLD' and abs(delta) < 5: continue # Скрываем мелкие
                    
                    f.write(f"{sym:<8} {cw:<8.1f} {tw:<8.1f} ${delta:<11.2f} {act}\n")
                    
                f.write("\n" + "="*60 + "\n")
                f.write("🚀 EXECUTION PLAN (ORDERS):\n")
                
                if not orders:
                    f.write("No actions required. Portfolio is balanced.\n")
                else:
                    for i, order in enumerate(orders, 1):
                        f.write(f"{i}. {order['side'].upper()} {order['symbol']}\n")
                        f.write(f"   Amount: ${order['amount_usd']:.2f} (~{order['amount_coin']:.4f} coins)\n")
                        f.write(f"   Reason: {order['reason']}\n\n")
                        
            return str(path)
            
        except Exception as e:
            print(f"Error generating portfolio report: {e}")
            return None