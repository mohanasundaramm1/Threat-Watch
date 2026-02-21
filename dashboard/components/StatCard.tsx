import { LucideIcon } from 'lucide-react';

interface StatCardProps {
  title: string;
  value: string | number;
  icon: LucideIcon;
  description?: string;
  trend?: {
    value: string;
    positive: boolean;
  };
  colorClass?: string;
  glowColor?: 'cyan' | 'red' | 'green' | 'purple';
}

export default function StatCard({ 
  title, 
  value, 
  icon: Icon, 
  description,
  trend,
  colorClass = 'from-cyan-500 to-blue-600',
  glowColor = 'cyan'
}: StatCardProps) {
  const glowClasses = {
    cyan: 'glow-cyan',
    red: 'glow-red',
    green: 'glow-green',
    purple: 'shadow-purple-500/30'
  };

  return (
    <div className="stat-card rounded-xl p-6 card-gradient-border card-hover relative overflow-hidden group">
      {/* Animated background glow */}
      <div className={`absolute inset-0 bg-gradient-to-br ${colorClass} opacity-0 group-hover:opacity-10 transition-opacity duration-500`}></div>
      
      <div className="flex items-start justify-between relative z-10">
        <div className="flex-1">
          <p className="text-xs font-bold text-slate-400 mb-2 uppercase tracking-wider">{title}</p>
          <p className="text-4xl font-black text-white mb-2 tracking-tight">
            {value}
          </p>
          {description && (
            <p className="text-sm text-slate-400 font-medium">{description}</p>
          )}
          {trend && (
            <div className={`inline-flex items-center gap-1.5 mt-3 px-3 py-1.5 rounded-lg text-xs font-bold ${
              trend.positive ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'
            }`}>
              {trend.value}
            </div>
          )}
        </div>
        <div className={`bg-gradient-to-br ${colorClass} p-4 rounded-xl ${glowClasses[glowColor]}`}>
          <Icon className="w-8 h-8 text-white" />
        </div>
      </div>
    </div>
  );
}
