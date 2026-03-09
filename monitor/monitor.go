package monitor

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rivo/tview"
	"libvirt.org/go/libvirt"

	"qemu-auto-ballooning/config"
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

	go func() {
		ticker := time.NewTicker(time.Duration(2) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				table.Clear()
				table.SetCell(0, 0, tview.NewTableCell("Name").SetAlign(tview.AlignCenter))
				updateData(table)
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
	conn, err := libvirt.NewConnect(cfg.Url)
	if err != nil {
		slog.Error("Failed to connect to hypervisor", "error", err)
	}
	defer conn.Close()

	// domains, err := conn.ListAllDomains(0)
	domains, err := conn.ListAllDomains(libvirt.CONNECT_LIST_DOMAINS_RUNNING)
	if err != nil {
		slog.Error("Failed to get active domains: %v", err)
	}

	for n, domain := range domains {

		domainName, err := domain.GetName()
		if err != nil {
			slog.Error("Failed to get domain name: %v", err)
		}

		// table.SetCell(n, 0, tview.NewTableCell(domainName).SetAlign(tview.AlignCenter))
		table.SetCell(n+1, 0, tview.NewTableCell(domainName))
	}
}
