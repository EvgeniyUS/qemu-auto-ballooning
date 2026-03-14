package monitor

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"qemu-auto-ballooning/config"
	"qemu-auto-ballooning/service"
)

var (
	cfg      config.Config
	vcpuTime uint64
	debug    bool
)

func Run() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		os.Exit(0)
	}()

	app := tview.NewApplication()
	table := tview.NewTable().SetBorders(true)
	table.SetBordersColor(tcell.ColorDimGray)

	go func() {
		ticker := time.NewTicker(time.Duration(2) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				updateData(table)
				app.Draw()
			case <-sigChan:
				return
			}
		}
	}()
	if err := app.SetRoot(table, true).Run(); err != nil {
		panic(err)
	}
}

func updateData(table *tview.Table) {
	conn, domains, err := service.ListAllDomains()
	if err != nil {
		return
	}
	defer conn.Close()

	nodeMemoryUsedPercent, err := service.GetNodeMemoryUsedPercent(conn)
	if err != nil {
		return
	}
	table.Clear()
	table.SetCell(0, 0, tview.NewTableCell("Domain name").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 1, tview.NewTableCell("Current").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 2, tview.NewTableCell("Maximum").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 3, tview.NewTableCell("Available").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 4, tview.NewTableCell("Unused").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 5, tview.NewTableCell("Usable").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 6, tview.NewTableCell("Rss").SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGoldenrod))
	table.SetCell(0, 7, tview.NewTableCell(fmt.Sprintf("Used (host: %.1f%%)", nodeMemoryUsedPercent)).SetTextColor(tcell.ColorDarkGoldenrod))

	var totalCurrent uint64
	var totalRss uint64
	var totalMaximum uint64
	var totalUsed uint64

	for n, domain := range domains {
		defer domain.Free()
		domainStats, err := service.GetDomainStats(&domain, conn)
		if err != nil {
			continue
		}
		if len(domainStats) == 0 {
			continue
		}
		defer domainStats[0].Domain.Free()

		domainName, err := domain.GetName()
		if err != nil {
			domainName = fmt.Sprintf("Domain-%d (not real)", n)
		}

		domainMemoryUsed := domainStats[0].Balloon.Available - domainStats[0].Balloon.Usable
		domainMemoryUsedPercent := float64(domainMemoryUsed) / float64(domainStats[0].Balloon.Available) * 100

		table.SetCell(n+1, 0, tview.NewTableCell(domainName))

		table.SetCell(n+1, 1, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Current/1024)).SetAlign(tview.AlignCenter))
		totalCurrent += domainStats[0].Balloon.Current

		table.SetCell(n+1, 2, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Maximum/1024)).SetAlign(tview.AlignCenter))
		totalMaximum += domainStats[0].Balloon.Maximum

		table.SetCell(n+1, 3, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Available/1024)).SetAlign(tview.AlignCenter))
		table.SetCell(n+1, 4, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Unused/1024)).SetAlign(tview.AlignCenter))
		table.SetCell(n+1, 5, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Usable/1024)).SetAlign(tview.AlignCenter))

		table.SetCell(n+1, 6, tview.NewTableCell(fmt.Sprintf("%d MB", domainStats[0].Balloon.Rss/1024)).SetAlign(tview.AlignCenter))
		totalRss += domainStats[0].Balloon.Rss

		table.SetCell(n+1, 7, tview.NewTableCell(fmt.Sprintf("%d (%.1f%%)", domainMemoryUsed/1024, domainMemoryUsedPercent)).SetAlign(tview.AlignCenter))
		totalUsed += domainMemoryUsed
	}
	domainCount := len(domains)
	table.SetCell(domainCount+1, 0, tview.NewTableCell("TOTAL").SetAlign(tview.AlignRight))
	table.SetCell(domainCount+1, 1, tview.NewTableCell(fmt.Sprintf("%d MB", totalCurrent/1024)).SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGreen))
	table.SetCell(domainCount+1, 2, tview.NewTableCell(fmt.Sprintf("%d MB", totalMaximum/1024)).SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkRed))
	table.SetCell(domainCount+1, 3, tview.NewTableCell(""))
	table.SetCell(domainCount+1, 4, tview.NewTableCell(""))
	table.SetCell(domainCount+1, 5, tview.NewTableCell(""))
	table.SetCell(domainCount+1, 6, tview.NewTableCell(fmt.Sprintf("%d MB", totalRss/1024)).SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGray))
	table.SetCell(domainCount+1, 7, tview.NewTableCell(fmt.Sprintf("%d MB", totalUsed/1024)).SetAlign(tview.AlignCenter).SetTextColor(tcell.ColorDarkGreen))
}
