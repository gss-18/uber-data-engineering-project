# Dashboard Upgrade Testing Checklist

## Visual Testing
- [ ] KPI cards display correctly on desktop and laptop
- [ ] Charts render with readable labels and legends
- [ ] AI panel and chat match the dashboard theme
- [ ] Status cards show EventHub, pipeline, AI, and serving state
- [ ] No text is cut off or overlapping

## Responsiveness Testing
- [ ] Desktop: all sections visible without awkward wrapping
- [ ] Laptop: KPI cards remain readable
- [ ] Tablet/mobile: content stacks without horizontal scrolling
- [ ] Tables remain usable on narrow screens

## Functionality Testing
- [ ] Ride booking still sends events to EventHub
- [ ] Refresh clears cached dashboard data and AI insights
- [ ] AI insights generate from aggregate dashboard data
- [ ] AI chat rejects questions outside supplied dashboard context
- [ ] Pipeline Control tab still authenticates and triggers actions

## Error Handling Testing
- [ ] Missing Gemini key disables AI without crashing
- [ ] Missing Streamlit secrets falls back to `.env`
- [ ] Databricks/API failures show friendly Streamlit messages
- [ ] Empty aggregate result sets show empty states

## Security Testing
- [ ] No hardcoded credentials
- [ ] `.env` and `.streamlit/secrets.toml` are ignored by git
- [ ] AI context excludes raw passenger and driver names
- [ ] No text-to-SQL or write actions are exposed through AI
