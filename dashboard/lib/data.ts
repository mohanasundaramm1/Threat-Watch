import type { ThreatDomain, DashboardStats, ModelMeta } from '@/types';

export async function loadThreatData(): Promise<ThreatDomain[]> {
  try {
    const response = await fetch('/data/threats.json');
    if (!response.ok) throw new Error('Failed to load threat data');
    return await response.json();
  } catch (error) {
    console.error('Error loading threat data:', error);
    return [];
  }
}

export async function loadHighRiskThreats(): Promise<ThreatDomain[]> {
  try {
    const response = await fetch('/data/threats_high_risk.json');
    if (!response.ok) throw new Error('Failed to load high risk threats');
    return await response.json();
  } catch (error) {
    console.error('Error loading high risk threats:', error);
    return [];
  }
}

export async function loadRecentThreats(): Promise<ThreatDomain[]> {
  try {
    const response = await fetch('/data/threats_recent.json');
    if (!response.ok) throw new Error('Failed to load recent threats');
    return await response.json();
  } catch (error) {
    console.error('Error loading recent threats:', error);
    return [];
  }
}

export async function loadStatistics(): Promise<DashboardStats | null> {
  try {
    const response = await fetch('/data/statistics.json');
    if (!response.ok) throw new Error('Failed to load statistics');
    return await response.json();
  } catch (error) {
    console.error('Error loading statistics:', error);
    return null;
  }
}

export function searchDomains(domains: ThreatDomain[], query: string): ThreatDomain[] {
  const lowerQuery = query.toLowerCase().trim();
  if (!lowerQuery) return domains;

  return domains.filter(d =>
    d.domain.toLowerCase().includes(lowerQuery) ||
    d.url.toLowerCase().includes(lowerQuery)
  );
}

export function filterByRiskLevel(domains: ThreatDomain[], riskLevel: string): ThreatDomain[] {
  if (riskLevel === 'ALL') return domains;
  return domains.filter(d => d.riskLevel === riskLevel);
}

export async function loadModelMeta(): Promise<ModelMeta | null> {
  try {
    const response = await fetch('/data/model_meta.json');
    if (!response.ok) throw new Error('Failed to load model metadata');
    return await response.json();
  } catch (error) {
    console.error('Error loading model metadata:', error);
    return null;
  }
}

