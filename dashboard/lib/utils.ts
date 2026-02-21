export function formatDate(dateString: string): string {
  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch {
    return dateString;
  }
}

export function getRiskColor(riskScore: number | undefined): string {
  if (riskScore === undefined) return 'bg-gray-500';
  if (riskScore >= 0.75) return 'bg-red-600';
  if (riskScore >= 0.50) return 'bg-orange-500';
  if (riskScore >= 0.30) return 'bg-yellow-500';
  return 'bg-green-500';
}

export function getRiskLevelColor(riskLevel: string): string {
  switch (riskLevel) {
    case 'CRITICAL': return 'bg-red-600 text-white';
    case 'HIGH': return 'bg-orange-500 text-white';
    case 'MEDIUM': return 'bg-yellow-500 text-black';
    case 'LOW': return 'bg-green-500 text-white';
    default: return 'bg-gray-500 text-white';
  }
}

export function getThreatTypeColor(threatType: string): string {
  switch (threatType.toLowerCase()) {
    case 'phishing': return 'bg-purple-600 text-white';
    case 'malware': return 'bg-red-600 text-white';
    case 'suspicious': return 'bg-orange-500 text-white';
    case 'benign': return 'bg-green-500 text-white';
    default: return 'bg-gray-500 text-white';
  }
}

export function truncateDomain(domain: string, maxLength: number = 40): string {
  if (domain.length <= maxLength) return domain;
  return domain.substring(0, maxLength) + '...';
}

export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

export function formatPercentage(value: number, total: number): string {
  if (total === 0) return '0%';
  return ((value / total) * 100).toFixed(1) + '%';
}

